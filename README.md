# pawchat-kv

Store clé-valeur Rust embarqué, taillé sur mesure pour deux besoins précis
de PawChat : le rate-limiting et le cache de révocation de
`credential_version`. Ce n'est **pas** un Redis-killer généraliste — voir
`docs/kv-store-research-pawchat-design.md` (§6) pour la recherche complète
qui a mené à ce choix plutôt que Dragonfly/KeyDB/Garnet/Kvrocks/Redis.

C'est une bibliothèque (`crate-type` par défaut, `lib.rs`), pas un service
réseau : elle est prévue pour être embarquée directement dans le futur
`pawchat-auth` (`docs/auth-microservice-rust-plan.md`), pas déployée comme
process séparé.

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
      ShardedTtlMap<K, V>   (générique, src/table.rs)
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
  (`src/table.rs`) : une tâche `tokio::spawn` périodique fait un
  `retain()` sur la table entière. À l'échelle de PawChat (quelques
  milliers de clés actives), un scan complet périodique est largement
  assez rapide et beaucoup plus simple à auditer qu'une structure
  d'éviction plusélaborée.

En résumé : `moka` aurait été un choix défendable pour un cache générique ;
`DashMap` + une petite couche TTL maison donne un contrôle total et plus
simple sur la sémantique exacte (fenêtre glissante exacte, purge active)
dont ce crate a besoin.

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

## Ce qui est volontairement exclu (§6.5 du document)

- **Protocole réseau RESP/Memcached.** Aucun serveur, aucun parsing de
  protocole exposé au réseau — c'est une bibliothèque embarquée.
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
cargo test      # 19 tests d'intégration + 2 doctests
cargo clippy --all-targets -- -D warnings
cargo bench     # baseline criterion pour incr_and_check
```
