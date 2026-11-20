# Blocking return types and Spring MVC support

## Decision

Add synchronous return-type support as a second entry path over the existing coalescing algorithm. Do not build a second Redis protocol, bucket format, key resolver, or codec for servlet applications.

The initial implementation should support ordinary synchronous methods annotated with `@Coalesce` in Spring MVC applications. It should not claim support for servlet asynchronous return types such as `Callable`, `DeferredResult`, `CompletionStage`, or streaming response bodies until their lifecycle and cancellation semantics are designed separately.

The existing `Mono` and `Flux` paths remain reactive. Both paths must use the same `CoalesceCoordinator`, `CoalesceKeys` key construction, envelope format, namespace rules, and `CoalesceCodec`. A WebFlux pod and an MVC pod configured against the same Redis deployment must be able to read one another's entries and participate in the same leader/follower protocol.

## Proposed execution model

Keep the current reactive coordinator SPI as the single source of truth. Add a small blocking facade used only by the synchronous aspect path. It may compose the existing `Mono` operations and wait at the outer boundary, but it must never block inside the WebFlux path.

The synchronous path should follow the same state machine as the reactive path:

1. Resolve annotation attributes and the effective namespace.
2. Check the Redis-backed global and namespace kill switch.
3. Resolve the key using the same `CoalesceKeyResolver` and `CoalesceKeys` convention.
4. Read the bucket before attempting the lock.
5. Return a fresh cached value immediately; serve stale data while one caller refreshes it.
6. If no usable value exists, attempt the shared Redis lease. The winner invokes the target method and publishes the same encoded envelope; followers wait for the shared result and retry the bucket read.
7. Release the lease on success and failure, and preserve the existing timeout, retry, payload-size, and failure behavior.

A blocking follower can wait on the existing coordinator's completion signal and poll fallback. The blocking facade should wait only on the servlet request thread, never on a Reactor event-loop thread. It must apply the configured `waitTimeout` to the whole follower operation and unwrap completion wrappers so the caller observes the original target exception rather than an incidental `RuntimeException` or `CompletionException`.

Do not implement a separate `RBlockingQueue`, local map, second topic, or servlet-only Redis key. Those would split the protocol and prevent mixed MVC/WebFlux pods from coalescing with one another.

## Threading and virtual threads

The project currently targets Java 17. Virtual threads are a Java 21 feature, so the library cannot assume that every consumer can use them.

The first blocking implementation should therefore be correct on ordinary servlet worker threads and document the resource trade-off: each follower occupies a request thread while waiting, up to the configured wait timeout. This is acceptable for an explicitly synchronous API, but deployments with high follower fan-out should size their servlet thread pool and timeout accordingly.

Virtual threads are an optional deployment-level improvement, not a library requirement. On Java 21+, an application can configure its servlet container to use virtual threads where its Spring Boot and container versions support that configuration. The starter should not create or own a global executor, silently switch the application's threading model, or require Java 21 just to enable MVC support.

Avoid calling `block()` from shared helper code that might also be reached by the reactive path. Keep the blocking boundary explicit and isolated in the synchronous aspect branch. If the implementation needs a scheduler to bridge an operation, use a bounded, clearly owned execution boundary and test that it cannot run blocking work on a Reactor event loop.

## Header-based key resolution in servlet applications

WebFlux header capture uses Reactor Context because request processing moves between event-loop threads. That mechanism must remain unchanged.

For Spring MVC, add a servlet-specific capture component that reads configured request headers at the servlet request boundary and stores an immutable snapshot in request attributes. The resolver used by the synchronous aspect can read that snapshot from `RequestContextHolder` while the request is active.

Requirements:

- Register the servlet capture component only in a servlet web application; do not register it in reactive applications.
- Capture only the header names configured by the annotation or the existing header-capture configuration. Do not copy all request headers into a thread-local.
- Preserve the existing canonicalization and key derivation rules so equivalent header values produce the same key in MVC and WebFlux.
- Handle a missing request context deterministically. If `headerKeys` are configured but no servlet request is bound, fail with a clear configuration/runtime error rather than silently generating a key that collides with requests using a header.
- Clear request attributes in the filter's `finally` path. Do not retain request or header state in static fields.
- Document that `RequestContextHolder` is thread-bound. Async servlet dispatch, work handed to another executor, and methods invoked outside an HTTP request need explicit context propagation or an exchange/request argument; they must not be implied to work automatically.

An explicitly supplied request/header argument in SpEL remains the most portable option for service methods whose execution can move across threads. The servlet capture mechanism is a convenience for synchronous request-bound call chains, not a general context propagation system.

## Mixed MVC and WebFlux deployments

The shared Redis state is the compatibility boundary. Mixed deployments are supported only when all pods agree on:

- the same namespace and key derivation rules;
- the same codec and envelope version;
- compatible TTL and payload-size configuration;
- the same Redis cluster and coordinator implementation.

A synchronous MVC caller may become the leader while a WebFlux caller follows, and vice versa. Both must observe the same cached payload and status transitions. Add integration tests that exercise this cross-runtime behavior rather than testing two isolated Redis namespaces.

The servlet header capture filter and WebFlux `HeaderCaptureFilter` are separate adapters. They feed the same key resolver with the same immutable header representation; neither changes the Redis key format.

## Test plan

Before enabling the feature by default, cover:

1. A synchronous MVC method returns its original value and is invoked once for concurrent callers with the same key.
2. A synchronous follower receives the leader's result, and a follower timeout raises the library's existing timeout exception.
3. A cached fresh value avoids invoking the target; a stale value is returned while one background refresh is started.
4. Empty/null and exceptional target results follow explicitly documented semantics and do not strand the lock.
5. A custom `CoalesceCodec` and custom `CoalesceCoordinator` continue to be honored.
6. Configured servlet headers contribute to the same key as equivalent WebFlux header values.
7. Missing servlet request context with configured `headerKeys` fails clearly, and captured headers are cleared after the request.
8. MVC and WebFlux application contexts pointed at the same Redis instance can share a leader result and cached entry.
9. The synchronous branch never blocks a Reactor event-loop thread.
10. Existing `Mono`/`Flux` tests and the Java 17 toolchain continue to pass.

## Scope boundaries

The first release should cover synchronous return values only. Reactive return types keep their existing behavior. Servlet async types, cancellation propagation, streaming responses, and automatic context propagation across arbitrary executors remain separate design work.

This keeps the feature useful to conventional Spring MVC applications without changing the library's distributed protocol or making Java 21 a minimum runtime.
