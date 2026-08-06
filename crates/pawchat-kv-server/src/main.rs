use std::sync::Arc;
use std::time::Duration;

use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Request, State},
    http::{header, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use pawchat_kv::{RateLimiter, RevocationCache};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tower::ServiceBuilder;
use tracing_subscriber::EnvFilter;

const WINDOW_SECS_MAX: u64 = 86_400;
const PORT_DEFAUT: u16 = 3210;
const LIMITE_CONCURRENCE_DEFAUT: usize = 256;

struct AppState {
    limiter: RateLimiter,
    revocation: RevocationCache,
    secret: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .init();

    let secret =
        secret_depuis_env(std::env::var("PAWCHAT_KV_INTERNAL_SECRET")).unwrap_or_else(|erreur| {
            tracing::error!(
                "{erreur} — pawchat-kv-server expose un cache de revocation de credentials \
                 et refuse de demarrer sans secret configure"
            );
            std::process::exit(1);
        });

    let chemin_db = std::env::var("PAWCHAT_KV_DB_PATH")
        .ok()
        .filter(|p| !p.is_empty());
    let revocation = match chemin_db.as_deref() {
        Some(chemin) => RevocationCache::open(chemin).unwrap_or_else(|erreur| {
            tracing::error!(%erreur, chemin, "ouverture de la base redb impossible");
            std::process::exit(1);
        }),
        None => RevocationCache::new_in_memory(),
    };

    let state = Arc::new(AppState {
        limiter: RateLimiter::new(),
        revocation,
        secret,
    });

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(PORT_DEFAUT);
    let limite = limite_concurrence();
    tracing::info!(
        port,
        concurrency_limit = limite,
        persistant = state.revocation.is_persistent(),
        chemin_db = chemin_db.as_deref().unwrap_or("(memoire seule)"),
        secret_configure = true,
        secret_longueur = state.secret.len(),
        "configuration effective au demarrage"
    );

    let app = router(state);

    let addr = format!("0.0.0.0:{port}");
    tracing::info!(%addr, "pawchat-kv-server en ecoute");
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
    axum::serve(listener, app).await.expect("serve");
}

fn limite_concurrence() -> usize {
    std::env::var("PAWCHAT_KV_MAX_CONCURRENT_REQUESTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(LIMITE_CONCURRENCE_DEFAUT)
}

fn secret_depuis_env(v: Result<String, std::env::VarError>) -> Result<String, &'static str> {
    match v {
        Ok(s) if !s.is_empty() => Ok(s),
        _ => Err("PAWCHAT_KV_INTERNAL_SECRET manquant ou vide"),
    }
}

fn secret_valide(configure: &str, fourni: Option<&str>) -> bool {
    let Some(fourni) = fourni else {
        return false;
    };
    fourni.len() == configure.len() && bool::from(fourni.as_bytes().ct_eq(configure.as_bytes()))
}

#[tracing::instrument(level = "trace", skip_all, fields(path = %request.uri().path()))]
async fn authentifier(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let fourni = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    if secret_valide(&state.secret, fourni) {
        next.run(request).await
    } else {
        tracing::warn!("requete rejetee par l'authentification");
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"))],
            Json(serde_json::json!({ "error": "authentification requise" })),
        )
            .into_response()
    }
}

async fn gerer_surcharge(erreur: tower::BoxError) -> Response {
    tracing::warn!(%erreur, "requete rejetee — limite de resilience atteinte");
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({ "error": "serveur sature, reessayer" })),
    )
        .into_response()
}

fn router_protege(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/rate-limit/check", post(rate_limit_check))
        .route(
            "/revocation/:user_id",
            get(revocation_get).post(revocation_set),
        )
        .with_state(state)
}

fn router(state: Arc<AppState>) -> Router {
    let limite = limite_concurrence();

    let protege = ServiceBuilder::new()
        .layer(middleware::from_fn_with_state(state.clone(), authentifier))
        .layer(HandleErrorLayer::new(gerer_surcharge))
        .load_shed()
        .concurrency_limit(limite)
        .service(router_protege(state));

    Router::new()
        .route("/health", get(health))
        .fallback_service(protege)
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "ok": true, "service": "pawchat-kv-server" }))
}

#[derive(Deserialize)]
struct RateLimitRequest {
    key: String,
    limit: u32,
    window_secs: u64,
}

#[derive(Serialize)]
struct RateLimitResponse {
    allowed: bool,
}

#[tracing::instrument(
    level = "info",
    skip_all,
    fields(key = req.key.as_str(), limit = req.limit, window_secs = req.window_secs)
)]
async fn rate_limit_check(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RateLimitRequest>,
) -> Response {
    if req.key.is_empty() {
        return erreur(StatusCode::BAD_REQUEST, "key ne doit pas etre vide");
    }
    if req.window_secs == 0 || req.window_secs > WINDOW_SECS_MAX {
        return erreur(
            StatusCode::BAD_REQUEST,
            &format!("window_secs hors domaine (1..={WINDOW_SECS_MAX})"),
        );
    }

    let allowed = state
        .limiter
        .incr_and_check(&req.key, req.limit, Duration::from_secs(req.window_secs))
        .await;
    tracing::info!(allowed, "verification de rate limit");
    Json(RateLimitResponse { allowed }).into_response()
}

#[derive(Serialize)]
struct RevocationResponse {
    version: Option<u32>,
}

#[tracing::instrument(level = "info", skip_all, fields(user_id = user_id))]
async fn revocation_get(State(state): State<Arc<AppState>>, Path(user_id): Path<u64>) -> Response {
    let version = state.revocation.get_cv(user_id).await;
    tracing::info!(?version, "lecture de credential_version");
    Json(RevocationResponse { version }).into_response()
}

#[derive(Deserialize)]
struct RevocationSetRequest {
    version: u32,
}

#[tracing::instrument(level = "info", skip_all, fields(user_id = user_id, version = req.version))]
async fn revocation_set(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<u64>,
    Json(req): Json<RevocationSetRequest>,
) -> Response {
    match state.revocation.set_cv(user_id, req.version).await {
        Ok(()) => {
            tracing::info!("credential_version ecrit");
            Json(serde_json::json!({ "ok": true })).into_response()
        }
        Err(e) => {
            tracing::error!(raison = %e, "persistance du credential_version impossible");
            erreur(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
        }
    }
}

fn erreur(statut: StatusCode, message: &str) -> Response {
    (statut, Json(serde_json::json!({ "error": message }))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    const SECRET_TEST: &str = "secret-de-test";

    fn app() -> Router {
        router(Arc::new(AppState {
            limiter: RateLimiter::without_purge_task(),
            revocation: RevocationCache::new_in_memory(),
            secret: SECRET_TEST.to_string(),
        }))
    }

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn post(path: &str, body: serde_json::Value) -> HttpRequest<Body> {
        HttpRequest::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {SECRET_TEST}"))
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    fn post_sans_secret(path: &str, body: serde_json::Value) -> HttpRequest<Body> {
        HttpRequest::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    fn get_authentifie(path: &str) -> HttpRequest<Body> {
        HttpRequest::builder()
            .method("GET")
            .uri(path)
            .header("authorization", format!("Bearer {SECRET_TEST}"))
            .body(Body::empty())
            .unwrap()
    }

    fn get_sans_secret(path: &str) -> HttpRequest<Body> {
        HttpRequest::builder()
            .method("GET")
            .uri(path)
            .body(Body::empty())
            .unwrap()
    }

    #[test]
    fn secret_valide_accepte_le_bon_secret() {
        assert!(secret_valide("abc123", Some("abc123")));
    }

    #[test]
    fn secret_valide_refuse_un_mauvais_secret_meme_longueur() {
        assert!(!secret_valide("abc123", Some("xbc123")));
    }

    #[test]
    fn secret_valide_refuse_une_longueur_differente() {
        assert!(!secret_valide("abc123", Some("abc1234")));
        assert!(!secret_valide("abc123", Some("abc12")));
    }

    #[test]
    fn secret_valide_refuse_l_absence_de_secret() {
        assert!(!secret_valide("abc123", None));
    }

    #[test]
    fn secret_depuis_env_accepte_une_valeur_non_vide() {
        assert_eq!(secret_depuis_env(Ok("x".to_string())), Ok("x".to_string()));
    }

    #[test]
    fn secret_depuis_env_refuse_une_valeur_vide() {
        assert!(secret_depuis_env(Ok(String::new())).is_err());
    }

    #[test]
    fn secret_depuis_env_refuse_une_variable_absente() {
        assert!(secret_depuis_env(Err(std::env::VarError::NotPresent)).is_err());
    }

    #[tokio::test]
    async fn health_reste_public_sans_secret() {
        let response = app().oneshot(get_sans_secret("/health")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;
        assert_eq!(json["service"], "pawchat-kv-server");
    }

    #[tokio::test]
    async fn rate_limit_sans_secret_est_rejete_401() {
        let response = app()
            .oneshot(post_sans_secret(
                "/rate-limit/check",
                serde_json::json!({"key":"login:1.2.3.4","limit":5,"window_secs":60}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            "Bearer"
        );
    }

    #[tokio::test]
    async fn rate_limit_sans_aucun_header_est_rejete_401_pas_415() {
        let request = HttpRequest::builder()
            .method("POST")
            .uri("/rate-limit/check")
            .body(Body::empty())
            .unwrap();
        let response = app().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn revocation_get_sans_secret_est_rejete_401() {
        let response = app()
            .oneshot(get_sans_secret("/revocation/42"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn revocation_set_sans_secret_est_rejete_401() {
        let response = app()
            .oneshot(post_sans_secret(
                "/revocation/42",
                serde_json::json!({"version":3}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rate_limit_avec_mauvais_secret_est_rejete_401() {
        let request = HttpRequest::builder()
            .method("POST")
            .uri("/rate-limit/check")
            .header("content-type", "application/json")
            .header("authorization", "Bearer mauvais-secret")
            .body(Body::from(
                serde_json::json!({"key":"k","limit":5,"window_secs":60}).to_string(),
            ))
            .unwrap();
        let response = app().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rate_limit_autorise_jusqu_a_la_limite_puis_refuse() {
        let application = app();
        for _ in 0..3 {
            let response = application
                .clone()
                .oneshot(post(
                    "/rate-limit/check",
                    serde_json::json!({"key":"login:1.2.3.4","limit":3,"window_secs":60}),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(body_json(response).await["allowed"], true);
        }

        let response = application
            .oneshot(post(
                "/rate-limit/check",
                serde_json::json!({"key":"login:1.2.3.4","limit":3,"window_secs":60}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["allowed"], false);
    }

    #[tokio::test]
    async fn rate_limit_isole_les_cles_entre_elles() {
        let application = app();
        application
            .clone()
            .oneshot(post(
                "/rate-limit/check",
                serde_json::json!({"key":"a","limit":1,"window_secs":60}),
            ))
            .await
            .unwrap();
        let response = application
            .oneshot(post(
                "/rate-limit/check",
                serde_json::json!({"key":"b","limit":1,"window_secs":60}),
            ))
            .await
            .unwrap();
        assert_eq!(body_json(response).await["allowed"], true);
    }

    #[tokio::test]
    async fn rate_limit_window_secs_nul_rejete_400() {
        let response = app()
            .oneshot(post(
                "/rate-limit/check",
                serde_json::json!({"key":"k","limit":5,"window_secs":0}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rate_limit_window_secs_hors_domaine_rejete_400() {
        let response = app()
            .oneshot(post(
                "/rate-limit/check",
                serde_json::json!({"key":"k","limit":5,"window_secs": WINDOW_SECS_MAX + 1}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rate_limit_cle_vide_rejetee_400() {
        let response = app()
            .oneshot(post(
                "/rate-limit/check",
                serde_json::json!({"key":"","limit":5,"window_secs":60}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn revocation_inconnue_renvoie_version_null() {
        let response = app()
            .oneshot(get_authentifie("/revocation/7"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_json(response).await["version"],
            serde_json::Value::Null
        );
    }

    #[tokio::test]
    async fn revocation_ecriture_puis_lecture() {
        let application = app();
        let response = application
            .clone()
            .oneshot(post("/revocation/42", serde_json::json!({"version":3})))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["ok"], true);

        let response = application
            .oneshot(get_authentifie("/revocation/42"))
            .await
            .unwrap();
        assert_eq!(body_json(response).await["version"], 3);
    }

    #[tokio::test]
    async fn revocation_ecriture_ecrase_la_version_precedente() {
        let application = app();
        application
            .clone()
            .oneshot(post("/revocation/42", serde_json::json!({"version":3})))
            .await
            .unwrap();
        application
            .clone()
            .oneshot(post("/revocation/42", serde_json::json!({"version":4})))
            .await
            .unwrap();
        let response = application
            .oneshot(get_authentifie("/revocation/42"))
            .await
            .unwrap();
        assert_eq!(body_json(response).await["version"], 4);
    }

    #[tokio::test]
    async fn revocation_user_id_non_numerique_rejete_400() {
        let response = app()
            .oneshot(get_authentifie("/revocation/pas-un-nombre"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    fn routeur_lent(limite: usize) -> Router {
        async fn lent() -> &'static str {
            tokio::time::sleep(Duration::from_millis(200)).await;
            "ok"
        }

        let protege = ServiceBuilder::new()
            .layer(HandleErrorLayer::new(gerer_surcharge))
            .load_shed()
            .concurrency_limit(limite)
            .service(Router::<()>::new().route("/lent", get(lent)));

        Router::new()
            .route("/health", get(health))
            .fallback_service(protege)
    }

    fn requete(path: &str) -> HttpRequest<Body> {
        HttpRequest::builder()
            .uri(path)
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn au_dela_de_la_limite_de_concurrence_le_surplus_recoit_503() {
        let routeur = routeur_lent(1);

        let a = routeur.clone();
        let tache_a = tokio::spawn(async move { a.oneshot(requete("/lent")).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        let b = routeur.clone();
        let tache_b = tokio::spawn(async move { b.oneshot(requete("/lent")).await });

        let ra = tache_a.await.unwrap().unwrap();
        let rb = tache_b.await.unwrap().unwrap();
        let statuts = [ra.status(), rb.status()];
        assert!(
            statuts.contains(&StatusCode::SERVICE_UNAVAILABLE),
            "{statuts:?}"
        );
        assert!(statuts.contains(&StatusCode::OK), "{statuts:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sante_repond_pendant_la_saturation() {
        let routeur = routeur_lent(1);

        let occupe = routeur.clone();
        let tache_lente = tokio::spawn(async move { occupe.oneshot(requete("/lent")).await });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let sonde = routeur.clone();
        let reponse_sante = sonde.oneshot(requete("/health")).await.unwrap();
        assert_eq!(reponse_sante.status(), StatusCode::OK);

        tache_lente.await.unwrap().unwrap();
    }
}
