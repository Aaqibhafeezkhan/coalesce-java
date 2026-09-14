package net.bitsar.coalesce.demo.integration;

import net.bitsar.coalesce.annotation.Coalesce;
import net.bitsar.coalesce.demo.DemoMetrics;
import net.bitsar.coalesce.demo.Mode;
import net.bitsar.coalesce.demo.OrderDto;
import net.bitsar.coalesce.demo.OrderService;
import net.bitsar.coalesce.metrics.CoalesceMetrics;
import net.bitsar.coalesce.toggle.CoalesceToggle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonReactiveClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.TestPropertySource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Latency is pinned to a tight, fast distribution here so the downstream never lands in its
 * failure tail — these tests are about coalescing behaviour, not about the simulator.
 */
@SpringBootTest
@TestPropertySource(properties = {
        "demo.latency.mean-millis=120",
        "demo.latency.std-dev-millis=0",
        "demo.latency.max-millis=200"
})
class CoalesceIntegrationTest extends RedisAvailable {

    private static final Duration LIMIT = Duration.ofSeconds(30);

    @Autowired
    OrderService orders;

    @Autowired
    DemoMetrics demoMetrics;

    @Autowired
    CoalesceMetrics metrics;

    @Autowired
    CoalesceToggle toggle;

    @Autowired
    SwrProbe swr;

    @Autowired
    RedissonReactiveClient redisson;

    @BeforeEach
    void resetCounters() {
        demoMetrics.reset();
        metrics.reset();
        swr.resetRuns();
        // Buckets are fixed 1..10, so cached state would otherwise leak between tests — and
        // between runs, since Redis outlives the JVM.
        redisson.getKeys().deleteByPattern("coalesce:*").block(LIMIT);
    }

    private long executions() {
        return (long) demoMetrics.snapshot(Mode.COALESCED).get("downstreamExecutions");
    }

    // ---------- the runtime kill switch ----------

    /**
     * The switch has to be worth reaching for during an incident, which means it must take
     * the framework out of the path completely rather than merely stop caching. Twenty
     * concurrent callers that would otherwise share one execution each run their own.
     */
    @Test
    void switchingTheToggleOffStopsCoalescingWithoutARestart() {
        int bucket = 21;
        toggle.setActive(false).block(LIMIT);
        try {
            List<List<OrderDto>> results = Flux.range(0, 20)
                    .flatMap(i -> orders.loadCoalesced(bucket))
                    .collectList()
                    .block(LIMIT);

            assertThat(results).hasSize(20);
            assertThat(executions()).isEqualTo(20);
            // Nothing was read from or written to Redis on any of those calls.
            assertThat(metrics.snapshot()).containsEntry("cacheHits", 0L)
                    .containsEntry("followerWaits", 0L)
                    .containsEntry("leaderExecutions", 0L)
                    .containsEntry("bypassed", 20L);
        } finally {
            toggle.setActive(true).block(LIMIT);
        }
    }

    /**
     * The case the global switch cannot serve: one dependency is sick and the rest are
     * fine. Bypassing that namespace alone leaves every other annotated method shielded.
     */
    @Test
    void oneNamespaceCanBeBypassedWhileTheRestKeepCoalescing() {
        // The derived namespace is the full signature, so build it from the method rather
        // than hardcoding a string that would drift the moment the signature changes.
        String namespace = OrderService.class.getName() + ".loadCoalesced(int)";
        toggle.setActive(namespace, false).block(LIMIT);
        try {
            List<List<OrderDto>> bypassed = Flux.range(0, 10)
                    .flatMap(i -> orders.loadCoalesced(23))
                    .collectList()
                    .block(LIMIT);

            assertThat(bypassed).hasSize(10);
            assertThat(executions()).isEqualTo(10);

            // A different namespace is untouched and still coalesces to one execution.
            swr.resetRuns();
            // One id shared by all ten callers: a fresh id per call would be ten distinct
            // keys and would prove nothing about coalescing.
            String id = "untouched-" + UUID.randomUUID();
            List<String> shielded = Flux.range(0, 10)
                    .flatMap(i -> swr.value(id))
                    .collectList()
                    .block(LIMIT);
            assertThat(shielded).hasSize(10);
            assertThat(swr.runs()).isEqualTo(1);
        } finally {
            toggle.clearOverride(namespace).block(LIMIT);
        }
    }

    /** And switching it back on resumes coalescing on the very next call. */
    @Test
    void switchingItBackOnResumesCoalescing() {
        int bucket = 22;
        toggle.setActive(false).block(LIMIT);
        orders.loadCoalesced(bucket).block(LIMIT);
        toggle.setActive(true).block(LIMIT);

        List<List<OrderDto>> results = Flux.range(0, 10)
                .flatMap(i -> orders.loadCoalesced(bucket))
                .collectList()
                .block(LIMIT);

        assertThat(results).hasSize(10);
        // One bypassed call plus exactly one coalesced execution for the ten that followed.
        assertThat(executions()).isEqualTo(2);
        assertThat(metrics.snapshot()).containsEntry("bypassed", 1L);
    }

    @Test
    void concurrentCallsForOneBucketExecuteTheDownstreamExactlyOnce() {
        int bucket = 7;

        List<List<OrderDto>> results = Flux.range(0, 20)
                .flatMap(i -> orders.loadCoalesced(bucket))
                .collectList()
                .block(LIMIT);

        assertThat(results).hasSize(20);
        assertThat(executions()).isEqualTo(1);
        // Every caller got the leader's payload, not its own randomly generated one.
        assertThat(results).allSatisfy(r -> assertThat(r).isEqualTo(results.get(0)));
    }

    @Test
    void eachBucketIsCoalescedIndependently() {
        List<List<OrderDto>> results = Flux.range(1, 10)
                .flatMap(bucket -> Flux.range(0, 5).flatMap(i -> orders.loadCoalesced(bucket)))
                .collectList()
                .block(LIMIT);

        assertThat(results).hasSize(50);
        // Ten distinct keys, so ten executions for fifty requests.
        assertThat(executions()).isEqualTo(10);
        // bucket doubles as the order count, so the sizes prove keys did not cross-talk.
        assertThat(results.stream().map(List::size).distinct().sorted().toList())
                .containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    void cachedResultIsServedWithoutReExecuting() {
        int bucket = 3;

        List<OrderDto> first = orders.loadCoalesced(bucket).block(LIMIT);
        assertThat(executions()).isEqualTo(1);

        for (int i = 0; i < 5; i++) {
            assertThat(orders.loadCoalesced(bucket).block(LIMIT)).isEqualTo(first);
        }
        assertThat(executions()).isEqualTo(1);
        assertThat(metrics.snapshot().get("cacheHits")).isEqualTo(5L);
    }

    @Test
    void directModeExecutesEveryTimeAndIsTheBaseline() {
        int bucket = 4;

        Flux.range(0, 10).flatMap(i -> orders.loadDirect(bucket)).collectList().block(LIMIT);

        var direct = demoMetrics.snapshot(Mode.DIRECT);
        assertThat(direct.get("downstreamExecutions")).isEqualTo(10L);
        // Nothing is saved in direct mode, by construction — that is the point of it.
        assertThat(executions()).isZero();
    }

    @Test
    void cachedOrdersDecodeBackIntoDtosNotUntypedMaps() {
        int bucket = 5;

        List<OrderDto> first = orders.loadCoalesced(bucket).block(LIMIT);
        assertThat(first).hasSize(5).allSatisfy(o -> assertThat(o).isInstanceOf(OrderDto.class));

        // Second call round-trips through Redis and the codec. The decode type has to be a
        // parameterized List<OrderDto>, or the elements come back as LinkedHashMaps.
        List<OrderDto> cached = orders.loadCoalesced(bucket).block(LIMIT);
        assertThat(cached).hasSize(5).allSatisfy(o -> assertThat(o).isInstanceOf(OrderDto.class));
        assertThat(cached).isEqualTo(first);
    }

    @Test
    void aStaleReadIsServedImmediatelyAndTriggersOneBackgroundRefresh() throws Exception {
        String id = "swr-" + UUID.randomUUID();

        String first = swr.value(id).block(LIMIT);
        assertThat(swr.runs()).isEqualTo(1);

        Thread.sleep(1_200); // past freshTtl (1s), still inside staleTtl (60s)

        long startedAt = System.currentTimeMillis();
        String stale = swr.value(id).block(LIMIT);
        long elapsed = System.currentTimeMillis() - startedAt;

        assertThat(stale).isEqualTo(first);
        assertThat(elapsed).isLessThan(250); // not waiting for the ~300ms refresh

        Thread.sleep(1_000);
        assertThat(swr.runs()).isEqualTo(2);
        assertThat(metrics.snapshot().get("backgroundRefreshes")).isEqualTo(1L);
    }

    @Test
    void concurrentStaleReadsTriggerOnlyOneBackgroundRefresh() throws Exception {
        String id = "swr-" + UUID.randomUUID();

        swr.value(id).block(LIMIT);
        Thread.sleep(1_200);

        Flux.range(0, 10).flatMap(i -> swr.value(id)).collectList().block(LIMIT);
        Thread.sleep(1_000);

        assertThat(swr.runs()).isEqualTo(2);
    }

    @TestConfiguration
    static class Probes {

        @Bean
        SwrProbe swrProbe() {
            return new SwrProbe();
        }
    }

    /**
     * Short freshTtl so stale-while-revalidate is observable without a long sleep.
     *
     * <p>State is exposed through methods, not a public field: this bean is behind a CGLIB
     * proxy (that is how @Coalesce is applied), and reading a field through a proxy returns
     * the proxy subclass's own uninitialised field rather than the target's.
     */
    static class SwrProbe {

        private final AtomicInteger runs = new AtomicInteger();

        public int runs() {
            return runs.get();
        }

        public void resetRuns() {
            runs.set(0);
        }

        @Coalesce(key = "#id", freshTtlSeconds = "${swr.fresh-ttl:1}", staleTtlSeconds = "60",
                pendingTtlSeconds = "10", waitTimeoutSeconds = "15")
        public Mono<String> value(String id) {
            return Mono.defer(() -> {
                runs.incrementAndGet();
                return Mono.just("value-for-" + id + "@" + Instant.now())
                        .delayElement(Duration.ofMillis(300));
            });
        }
    }
}
