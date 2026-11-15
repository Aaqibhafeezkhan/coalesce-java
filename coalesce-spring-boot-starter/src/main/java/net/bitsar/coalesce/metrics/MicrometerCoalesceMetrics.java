package net.bitsar.coalesce.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;

/**
 * Exports the framework counters with a namespace tag and records follower wait duration
 * as a Micrometer timer. The inherited snapshot remains the source for the Actuator
 * kill-switch endpoint.
 */
public class MicrometerCoalesceMetrics extends CoalesceMetrics {

    private final MeterRegistry registry;

    public MicrometerCoalesceMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void leaderExecution() {
        super.leaderExecution();
        increment("coalesce.leader.executions", "unknown");
    }

    @Override
    public void leaderExecution(String namespace) {
        super.leaderExecution();
        increment("coalesce.leader.executions", namespace);
    }

    @Override
    public void cacheHit() {
        super.cacheHit();
        increment("coalesce.cache.hits", "unknown");
    }

    @Override
    public void cacheHit(String namespace) {
        super.cacheHit();
        increment("coalesce.cache.hits", namespace);
    }

    @Override
    public void followerWait(long millis) {
        super.followerWait(millis);
        recordWait("unknown", millis);
    }

    @Override
    public void followerWait(String namespace, long millis) {
        super.followerWait(millis);
        recordWait(namespace, millis);
    }

    @Override
    public void backgroundRefresh() {
        super.backgroundRefresh();
        increment("coalesce.background.refreshes", "unknown");
    }

    @Override
    public void backgroundRefresh(String namespace) {
        super.backgroundRefresh();
        increment("coalesce.background.refreshes", namespace);
    }

    @Override
    public void failedRetry() {
        super.failedRetry();
        increment("coalesce.failed.retries", "unknown");
    }

    @Override
    public void failedRetry(String namespace) {
        super.failedRetry();
        increment("coalesce.failed.retries", namespace);
    }

    @Override
    public void leaderTakeover() {
        super.leaderTakeover();
        increment("coalesce.leader.takeovers", "unknown");
    }

    @Override
    public void leaderTakeover(String namespace) {
        super.leaderTakeover();
        increment("coalesce.leader.takeovers", namespace);
    }

    @Override
    public void timeout() {
        super.timeout();
        increment("coalesce.timeouts", "unknown");
    }

    @Override
    public void timeout(String namespace) {
        super.timeout();
        increment("coalesce.timeouts", namespace);
    }

    @Override
    public void payloadTooLarge() {
        super.payloadTooLarge();
        increment("coalesce.payloads.too.large", "unknown");
    }

    @Override
    public void payloadTooLarge(String namespace) {
        super.payloadTooLarge();
        increment("coalesce.payloads.too.large", namespace);
    }

    @Override
    public void bypass() {
        super.bypass();
        increment("coalesce.bypassed", "unknown");
    }

    @Override
    public void bypass(String namespace) {
        super.bypass();
        increment("coalesce.bypassed", namespace);
    }

    private void increment(String name, String namespace) {
        Counter.builder(name)
                .tag("namespace", normalize(namespace))
                .register(registry)
                .increment();
    }

    private void recordWait(String namespace, long millis) {
        Timer.builder("coalesce.follower.wait")
                .tag("namespace", normalize(namespace))
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry)
                .record(Math.max(0, millis), TimeUnit.MILLISECONDS);
    }

    private static String normalize(String namespace) {
        return namespace == null || namespace.isBlank() ? "unknown" : namespace;
    }
}
