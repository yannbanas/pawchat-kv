# pawchat-kv

Store clé-valeur Rust embarqué, taillé sur mesure pour deux besoins précis
de PawChat : le rate-limiting et le cache de révocation de
`credential_version`. Ce n'est **pas** un Redis-killer généraliste — voir
[`docs/kv-store-research-pawchat-design.md`](https://github.com/mairie-creusot/pawchat/blob/main/docs/kv-store-research-pawchat-design.md)
(§6, dépôt `mairie-creusot/pawchat`) pour la recherche complète qui a mené à
ce choix plutôt que Dragonfly/KeyDB/Garnet/Kvrocks/Redis.

## Structure du dépôt

Workspace Cargo à deux crates :

| Crate | Rôle |
|---|---|
| `crates/pawchat-kv-core` | La bibliothèque (`RateLimiter`, `RevocationCache`). Nom de lib : `pawchat_kv`. Destinée à être embarquée directement dans le futur `pawchat-auth` (`docs/auth-microservice-rust-plan.md`). |
| `crates/pawchat-kv-server` | Le binaire « pont Phase 0 » : expose le cœur en HTTP interne minimaliste (`axum`), pour les appelants hors-process. |

Le serveur est exactement ce que le document de conception anticipe au §6.2 :
« si `pawchat-app` (Next.js) doit y accéder un jour… une API HTTP interne
minimaliste sur `axum` suffit — pas besoin de reproduire RESP ». Il n'y a
donc ni protocole RESP, ni clustering, ni exposition publique : c'est un pont
interne, derrière un secret partagé.

## Pourquoi construire plutôt qu'adopter

Rate-limiting et cache `credential_version` sont tous les deux : des paires
clé → petit entier/blob, avec TTL (rate-limit) ou invalidation événementielle
(`credential_version`), à très faible volume, lus très souvent et écrits
rarement. C'est un sous-ensemble minuscule de ce que Redis et ses
alternatives couvrent — les adopter ici serait déployer un process externe
entier (surface d'attaque réseau, supervision, mises à jour de sécurité)
pour un besoin qu'une bibliothèque interne de quelques centaines de lignes
couvre aussi bien, sans le coût opérationnel.

## Architecture

Deux structures publiques, un même moteur générique interne :

```
RateLimiter          RevocationCache
(fenêtre glissante,  (credential_version,
 jamais persisté)     persisté via redb)
       │                     │
       └────────┬────────────┘
                 │
      ShardedTtlMap<K, V>   (générique,
                             crates/pawchat-kv-core/src/table.rs)
      = DashMap<K, StoredEntry<V>>
        + tâche tokio::spawn de purge active périodique
```

Chaque structure instancie sa **propre** `ShardedTtlMap` (pas une seule
table hétérogène partagée entre les deux types de valeurs — mélanger
`SlidingWindow` et `u32` dans un seul `DashMap<String, enum Value>` aurait
demandé un `enum`/`Box<dyn Any>` sans bénéfice réel à cette échelle). C'est
ce que le document de conception appelle « même moteur, deux structures
logiques » : une seule implémentation du moteur sharded+TTL+purge, deux
tables indépendantes avec des politiques d'éviction différentes.

### `DashMap` plutôt que `moka` — choix tranché

Le document de conception laissait ce choix ouvert (§6.3). Décision : **`DashMap`**.

- Le besoin réel est trivial : compteurs (`incr_and_check`) et entiers
  (`credential_version`), pas de politique d'éviction LRU/LFU sophistiquée,
  pas de coût de calcul par entrée à amortir. `moka` apporte une machinerie
  (éviction basée sur TinyLFU, expiration paresseuse par défaut, API
  orientée cache plutôt que map) qui résout des problèmes que ce crate n'a
  pas.
- Le `RateLimiter` a besoin d'une opération **read-check-write atomique par
  clé** (lire le nombre de hits dans la fenêtre, décider, puis écrire) —
  c'est exactement ce que `DashMap::entry()` donne nativement en tenant le
  verrou du shard pendant toute la closure. Reproduire cette même garantie
  avec `moka` (dont l'API est pensée pour get/insert indépendants, pas pour
  des mutations atomiques composées) aurait demandé un verrou applicatif
  par-dessus — perdant l'essentiel de l'intérêt de `moka`.
- `DashMap` est une dépendance plus petite et plus prévisible : verrouillage
  par shard (`RwLock` interne), déjà éprouvé en production dans
  l'écosystème Rust (`tokio`, `rustc`), sans dépendre d'un scheduler
  d'éviction en tâche de fond supplémentaire à comprendre et déboguer.
- Le TTL et la purge active sont ici implémentés à la main
  (`crates/pawchat-kv-core/src/table.rs`) : une tâche `tokio::spawn`
  périodique fait un
  `retain()` sur la table entière. À l'échelle de PawChat (quelques
  milliers de clés actives), un scan complet périodique est largement
  assez rapide et beaucoup plus simple à auditer qu'une structure
  d'éviction plusélaborée.

En résumé : `moka` aurait été un choix défendable pour un cache générique ;
`DashMap` + une petite couche TTL maison donne un contrôle total et plus
simple sur la sémantique exacte (fenêtre glissante exacte, purge active)
dont ce crate a besoin.

### Sérialisation sur disque : `postcard`, pas `bincode`

Le `CvRecord` persisté dans `redb` est encodé avec `postcard`. `bincode`
était le choix initial, mais l'intégralité du crate est marquée
*unmaintained* par RUSTSEC (RUSTSEC-2025-0141, `patched = []` : aucune
version n'y échappe), ce qui fait échouer le job `cargo audit --deny warnings`
de la CI. `postcard` est maintenu, `no_std`+`alloc`, pilote serde de la même
façon, et produit un encodage plus compact — le remplacement tient en deux
appels (`to_allocvec`/`from_bytes`). Le format sur disque n'est pas
compatible avec l'ancien, ce qui est sans conséquence : aucun fichier `redb`
de ce crate n'existe encore en production.

### Horloge : `tokio::time::Instant`, pas `std::time::Instant`

Tout le crate utilise `tokio::time::Instant` (qui se comporte comme
`std::time::Instant` en production) plutôt que le type `std` directement.
Raison : ça permet aux tests d'utiliser `#[tokio::test(start_paused = true)]`
et `tokio::time::advance(...)` pour piloter précisément le temps (rollover
de fenêtre, purge par TTL) sans `sleep` réel — indispensable pour des tests
déterministes et rapides sur un comportement intrinsèquement temporel.

## API

```rust
use pawchat_kv::{RateLimiter, RevocationCache};
use std::time::Duration;

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
// Rate limiting : fenêtre glissante exacte (pas un compteur à fenêtre fixe
// façon INCR+EXPIRE, qui peut laisser passer 2x la limite à la frontière
// entre deux fenêtres).
let limiter = RateLimiter::new();
if limiter.incr_and_check("login:1.2.3.4", 5, Duration::from_secs(60)).await {
    // requête autorisée
} else {
    // 429 Too Many Requests
}

// Cache de révocation, persisté via redb pour réchauffer après redémarrage.
let cache = RevocationCache::open("revocation.redb")?;
cache.set_cv(42, 3).await?; // bump lors d'un changement de mot de passe
match cache.get_cv(42).await {
    Some(cv) if cv == token_cv => { /* token valide */ }
    _ => { /* token révoqué, ou pas en cache -> vérifier en base */ }
}
# Ok(())
# }
```

Sémantique clé :

| Cible du document (§6.4) | API réelle |
|---|---|
| `incr_and_check(key, limit, window) -> bool` | `RateLimiter::incr_and_check(&self, key: &str, limit: u32, window: Duration) -> bool` |
| `get_cv(user_id) -> Option<u32>` | `RevocationCache::get_cv(&self, user_id: u64) -> Option<u32>` |
| `set_cv(user_id, v)` | `RevocationCache::set_cv(&self, user_id: u64, version: u32) -> Result<(), KvError>` |

`set_cv` retourne un `Result` (contrairement à la signature exacte du
document) parce que, une fois persisté, il peut échouer côté `redb` — la
table en mémoire est mise à jour dans tous les cas, l'erreur ne signale
qu'un défaut de durabilité, pas de perte de service.

`RevocationCache::new_in_memory()` existe pour les tests / l'usage sans
fichier ; `RevocationCache::open(path)` charge la table depuis `redb` au
démarrage (« warm-load ») et persiste chaque `set_cv`/`invalidate` en
écriture synchrone (via `spawn_blocking`) avant de retourner.

Chaque structure expose `metrics() -> MetricsSnapshot` (hits, misses,
writes, purged, taille de table) instrumenté via `tracing` dès le premier
appel — pas ajouté après coup.

## `pawchat-kv-server` — le pont HTTP interne

Binaire `axum` qui expose le cœur à un appelant hors-process (typiquement
`pawchat-app`, Next.js). Volontairement minimaliste : quatre routes, JSON
sur HTTP, un secret partagé statique.

### Variables d'environnement

| Variable | Défaut | Rôle |
|---|---|---|
| `PAWCHAT_KV_INTERNAL_SECRET` | *(aucun — obligatoire)* | Secret partagé attendu en `Authorization: Bearer <secret>`. **Le serveur refuse de démarrer** s'il est absent ou vide : pas de mode dev permissif. |
| `PORT` | `3210` | Port d'écoute (choisi pour ne pas entrer en collision avec `chronotope-server`, qui utilise 3200). |
| `PAWCHAT_KV_DB_PATH` | `/data/revocation.redb` dans l'image Docker, *(vide)* sinon | Fichier `redb` du `RevocationCache`. Si vide/absent, le cache tourne en mémoire seule (aucune persistance entre redémarrages). |
| `PAWCHAT_KV_MAX_CONCURRENT_REQUESTS` | `256` | Plafond `tower` `concurrency_limit` ; le surplus est rejeté immédiatement en `503` par `load_shed` plutôt que mis en file. |
| `RUST_LOG` | `info` | Filtre `tracing-subscriber`. |

### Endpoints

Toutes les routes sauf `/health` exigent l'en-tête
`Authorization: Bearer $PAWCHAT_KV_INTERNAL_SECRET` (comparaison à temps
constant via `subtle`), sinon `401` + `WWW-Authenticate: Bearer`.

| Route | Corps de requête | Réponse |
|---|---|---|
| `POST /rate-limit/check` | `{"key": string, "limit": u32, "window_secs": u64}` | `{"allowed": bool}` |
| `GET /revocation/:user_id` | — | `{"version": u32 \| null}` |
| `POST /revocation/:user_id` | `{"version": u32}` | `{"ok": true}` |
| `GET /health` | — *(sans authentification)* | `{"ok": true, "service": "pawchat-kv-server"}` |

`version: null` signifie « pas en cache », pas « version 0 » ni « aucune
restriction » : l'appelant doit alors interroger la base (source de vérité)
puis réécrire la valeur via `POST /revocation/:user_id`.

`window_secs` est validé à la frontière (`1..=86400`), tout comme une `key`
vide et un `:user_id` non numérique — rejetés en `400` avant d'atteindre le
cœur. `/health` est hors des couches de résilience : il répond même quand
le reste est saturé (c'est ce que sonde le `HEALTHCHECK` Docker).

```bash
curl -s -X POST http://127.0.0.1:3210/rate-limit/check \
  -H "authorization: Bearer $PAWCHAT_KV_INTERNAL_SECRET" \
  -H 'content-type: application/json' \
  -d '{"key":"login:1.2.3.4","limit":5,"window_secs":60}'
# {"allowed":true}

curl -s -X POST http://127.0.0.1:3210/revocation/42 \
  -H "authorization: Bearer $PAWCHAT_KV_INTERNAL_SECRET" \
  -H 'content-type: application/json' -d '{"version":3}'
# {"ok":true}

curl -s http://127.0.0.1:3210/revocation/42 \
  -H "authorization: Bearer $PAWCHAT_KV_INTERNAL_SECRET"
# {"version":3}
```

### Docker

Image publiée sur `ghcr.io/mairie-creusot/pawchat-kv` (package **privé** :
ce service tient un cache de révocation de credentials, il n'a aucune raison
d'être public). Recette : cross-compile musl statique + `alpine:3.20` +
utilisateur non-root + `HEALTHCHECK` sur `/health`.

```bash
docker run -d --name pawchat-kv \
  -p 3210:3210 \
  -e PAWCHAT_KV_INTERNAL_SECRET=change-moi \
  -v pawchat-kv-data:/data \
  ghcr.io/mairie-creusot/pawchat-kv:latest
```

Le volume sur `/data` conserve le fichier `redb` de révocation entre deux
redémarrages ; sans lui, le cache repart froid (correct, mais chaque
première lecture par utilisateur retombe sur la base).

## Ce qui est volontairement exclu (§6.5 du document)

- **Protocole réseau RESP/Memcached.** `pawchat-kv-server` parle JSON sur
  HTTP, sur un réseau interne et derrière un secret partagé — aucun parsing
  de protocole binaire, aucune ambition de compatibilité client Redis.
- **Clustering / sharding distribué / réplication multi-nœud.** Le cache
  reste local à chaque processus ; si `pawchat-auth` tourne un jour en
  plusieurs répliques, la stratégie de cohérence (dégradation propre par
  réplique, ou pub/sub léger) sera tranchée à ce moment-là, pas ici.
- **État éphémère des salons VR / métaverse temps réel.** Couvert par
  SpacetimeDB ailleurs dans le repo — pas dupliqué ici.
- **Moteur LSM façon RocksDB/Kvrocks.** `redb` (arbre B+ transactionnel,
  100 % Rust, zéro dépendance C++) est utilisé, et uniquement pour
  `RevocationCache`. `RateLimiter` n'est jamais persisté.

## Dette technique explicite (par rapport à la version « complète » du §6.6)

Cette implémentation est une première passe solide, pas la version 7-11
jours-personne décrite dans le document. Ce qui est **fait** :

- `RateLimiter` : fenêtre glissante exacte (log de timestamps, pas
  d'approximation par pondération), atomique par clé, purge active testée.
- `RevocationCache` : persistance `redb` réelle, warm-load testé avec un
  vrai redémarrage simulé (drop + reopen du même fichier), écriture
  concurrente testée.
- Tests de charge concurrente sur runtime multi-thread réel (pas seulement
  `current_thread`), instrumentation `tracing` dès le début, un benchmark
  `criterion` baseline (single-key contendu, many-keys, concurrence
  multi-tâches).

Ce qui reste volontairement **simplifié** (à approfondir si ce crate va en
prod dans `pawchat-auth`) :

- `RevocationCache::set_cv` fait une transaction `redb` synchrone (via
  `spawn_blocking`) à chaque appel, pas un WAL batché avec snapshot
  périodique — suffisant tant que les écritures restent rares (changement
  de mot de passe, ban), mais pas conçu pour un burst d'écritures.
  Pas de test de corruption de fichier / recovery après crash au milieu
  d'une transaction (`redb` garantit l'atomicité transactionnelle, mais
  ça n'a pas été testé ici en conditions de panne simulée — kill -9,
  disque plein, etc.).
- Pas de benchmark comparatif contre un vrai serveur Redis (le document
  source indique explicitement que ce n'est pas l'objectif d'une première
  passe) — seulement une baseline `criterion` interne pour détecter les
  régressions futures.
- Pas de mécanisme de pub/sub inter-répliques pour `credential_version` :
  non applicable tant que `pawchat-auth` tourne en un seul processus.
- La tâche de purge du `RateLimiter` fait un scan complet (`retain`) de la
  table à chaque tick plutôt qu'une structure d'expiration en tas
  (min-heap par `expires_at`) — largement suffisant au volume actuel de
  PawChat, mais deviendrait un point à revoir si le nombre de clés actives
  grimpait de plusieurs ordres de grandeur.

## Tests

```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets    # 17 tests cœur + 24 tests serveur
cargo test --workspace --doc            # 2 doctests
cargo bench -p pawchat-kv-core          # baseline criterion pour incr_and_check
```

Les quatre premières commandes sont exactement ce que fait
`.github/workflows/ci.yml` ; `cargo audit --deny warnings` s'y ajoute en CI.
