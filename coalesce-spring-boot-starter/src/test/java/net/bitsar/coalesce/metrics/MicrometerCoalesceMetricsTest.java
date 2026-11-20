package net.bitsar.coalesce.metrics;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerCoalesceMetricsTest {

    @Test
    void countersAreSeparatedByNamespace() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerCoalesceMetrics metrics = new MicrometerCoalesceMetrics(registry);

        metrics.leaderExecution("orders");
        metrics.leaderExecution("orders");
        metrics.leaderExecution("payments");
        metrics.cacheHit("orders");

        assertThat(registry.get("coalesce.leader.executions")
                .tag("namespace", "orders").counter().count()).isEqualTo(2);
        assertThat(registry.get("coalesce.leader.executions")
                .tag("namespace", "payments").counter().count()).isEqualTo(1);
        assertThat(registry.get("coalesce.cache.hits")
                .tag("namespace", "orders").counter().count()).isEqualTo(1);
    }

    @Test
    void allOperationalCountersAreExported() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerCoalesceMetrics metrics = new MicrometerCoalesceMetrics(registry);

        metrics.backgroundRefresh("orders");
        metrics.failedRetry("orders");
        metrics.leaderTakeover("orders");
        metrics.timeout("orders");
        metrics.payloadTooLarge("orders");
        metrics.bypass("orders");

        assertCounter(registry, "coalesce.background.refreshes");
        assertCounter(registry, "coalesce.failed.retries");
        assertCounter(registry, "coalesce.leader.takeovers");
        assertCounter(registry, "coalesce.timeouts");
        assertCounter(registry, "coalesce.payloads.too.large");
        assertCounter(registry, "coalesce.bypassed");
    }

    @Test
    void followerWaitIsRecordedAsNamespaceTaggedTimerWithPercentiles() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerCoalesceMetrics metrics = new MicrometerCoalesceMetrics(registry);

        metrics.followerWait("orders", 125);

        Timer timer = registry.get("coalesce.follower.wait")
                .tag("namespace", "orders").timer();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(125);
    }

    @Test
    void legacyCallsRemainVisibleUnderUnknownNamespace() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerCoalesceMetrics metrics = new MicrometerCoalesceMetrics(registry);

        metrics.leaderExecution();
        metrics.followerWait(40);

        assertThat(registry.get("coalesce.leader.executions")
                .tag("namespace", "unknown").counter().count()).isEqualTo(1);
        assertThat(registry.get("coalesce.follower.wait")
                .tag("namespace", "unknown").timer().count()).isEqualTo(1);
        assertThat(metrics.snapshot().get("leaderExecutions")).isEqualTo(1);
    }

    private static void assertCounter(SimpleMeterRegistry registry, String name) {
        assertThat(registry.get(name).tag("namespace", "orders").counter().count()).isEqualTo(1);
    }
}
