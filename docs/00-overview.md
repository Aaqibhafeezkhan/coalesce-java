# @Coalesce: distributed reactive call coalescing + stale-while-revalidate

## What this is

A Spring AOP annotation for WebFlux methods returning `Mono<T>` or `Flux<T>` that:

1. Coalesces concurrent calls. If the same logical call is already in flight
   anywhere in the cluster, later callers wait for it and reuse the result instead of
   re-executing.
2. Caches the result for a configurable window, acting as a TPS shield in front
   of a slow or expensive downstream dependency.
3. Refreshes stale data in the background (stale-while-revalidate). Callers get
   an instant response from the last known-good value while, at most, one call
   refreshes it.

It is global-only: there is no per-pod in-memory tier. All coordination state
lives in Redis via Redisson, so every pod is stateless with respect to coalescing and
interchangeable/restartable at any time. (A local per-pod cache was considered and
explicitly dropped. See `06-implementation-checklist.md` for why, and when to revisit it.)

## The three Redis-backed objects

One deterministic key string (see `04-key-resolution.md`) names three separate
Redisson objects. Every pod computing the same string is operating on the same three
objects, and that is the entirety of what makes this distributed:

| Object | Redisson type | Purpose |
|---|---|---|
| Lock | `RLockReactive` | leader election: who gets to execute |
| Bucket | `RBucketReactive<byte[]>` | cached status + result payload |
| Topic | `RTopicReactive` (or sharded topic) | wake-up signal for waiting followers |

## The core rule that ties caching and coalescing together

**Every call checks the bucket before ever touching the lock.** This is the fix for
the bug where a caller arriving after the leader already released the lock would
re-execute from scratch instead of reading the perfectly good cached result sitting
next to it. See `02-aspect.md`, `coalesce()`.

## Files in this spec

- `01-annotation.md`: the `@Coalesce` annotation and what each field controls
- `02-aspect.md`: the full `CoalesceAspect`, covering entry point, leader path, follower
  wait loop, stale-while-revalidate branch
- `03-coordinator-and-codec.md`: `RedissonCoalesceCoordinator`, the pluggable
  `CoalesceCodec`, `CoalesceState`, and Spring Boot wiring
- `04-key-resolution.md`: deriving a deterministic key from SpEL args + HTTP headers,
  including the WebFlux Reactor Context nuance
- `05-cluster-considerations.md`: hash tags, sharded pub/sub, read mode, and what
  guarantees Redis Cluster does and does not give this design
- `06-implementation-checklist.md`: build order for the POC, what to test, and known
  open decisions to make explicitly rather than by accident
- `07-blocking-mvc-design.md`: proposed blocking facade, servlet header propagation,
  thread model, and compatibility contract for Spring MVC

## Non-goals / explicit caveats to keep in mind while building

- Not exactly-once. Redis Cluster failover has an inherent (rare) window where two
  pods can both believe they hold the lock. Annotated methods should be idempotent.
  This framework is best-effort deduplication and a TPS shield, not a distributed
  transaction primitive. Do not use it to protect non-idempotent operations (payments,
  side-effecting writes) without additional idempotency guarantees at the data layer.
- `Flux` support is for bounded streams only. The leader collects the whole `Flux`
  into a `List` before caching; there is no live multicast of an unbounded stream
  across pods.
- No local tier. Every follower pays a Redis round trip + deserialization even for
  same-pod concurrency. Acceptable for the POC; see `06-implementation-checklist.md`
  for when to reconsider.
