package net.bitsar.coalesce.metrics;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The leader/hit/wait ratio is the entire point of the framework — if it is not
 * visible there is no way to tell whether coalescing is actually happening.
 */
public class CoalesceMetrics {

    private final AtomicLong leaderExecutions = new AtomicLong();
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong followerWaits = new AtomicLong();
    private final AtomicLong backgroundRefreshes = new AtomicLong();
    private final AtomicLong failedRetries = new AtomicLong();
    private final AtomicLong leaderTakeovers = new AtomicLong();
    private final AtomicLong timeouts = new AtomicLong();
    private final AtomicLong payloadsTooLarge = new AtomicLong();
    private final AtomicLong bypassed = new AtomicLong();
    private final AtomicLong followerWaitMillisTotal = new AtomicLong();

    public void leaderExecution() {
        leaderExecutions.incrementAndGet();
    }

    public void leaderExecution(String namespace) {
        leaderExecution();
    }

    public void cacheHit() {
        cacheHits.incrementAndGet();
    }

    public void cacheHit(String namespace) {
        cacheHit();
    }

    public void followerWait(long millis) {
        followerWaits.incrementAndGet();
        followerWaitMillisTotal.addAndGet(millis);
    }

    public void followerWait(String namespace, long millis) {
        followerWait(millis);
    }

    public void backgroundRefresh() {
        backgroundRefreshes.incrementAndGet();
    }

    public void backgroundRefresh(String namespace) {
        backgroundRefresh();
    }

    public void failedRetry() {
        failedRetries.incrementAndGet();
    }

    public void failedRetry(String namespace) {
        failedRetry();
    }

    /** A waiter took the lock over after the previous leader's lease expired (crash recovery). */
    public void leaderTakeover() {
        leaderTakeovers.incrementAndGet();
    }

    public void leaderTakeover(String namespace) {
        leaderTakeover();
    }

    public void timeout() {
        timeouts.incrementAndGet();
    }

    public void timeout(String namespace) {
        timeout();
    }

    /** A result was computed but deliberately not cached because it was too big. */
    public void payloadTooLarge() {
        payloadsTooLarge.incrementAndGet();
    }

    public void payloadTooLarge(String namespace) {
        payloadTooLarge();
    }

    /** An annotated call ran straight through because the runtime toggle is off. */
    public void bypass() {
        bypassed.incrementAndGet();
    }

    public void bypass(String namespace) {
        bypass();
    }

    public Map<String, Long> snapshot() {
        long waits = followerWaits.get();
        return Map.of(
                "leaderExecutions", leaderExecutions.get(),
                "cacheHits", cacheHits.get(),
                "followerWaits", waits,
                "backgroundRefreshes", backgroundRefreshes.get(),
                "failedRetries", failedRetries.get(),
                "leaderTakeovers", leaderTakeovers.get(),
                "timeouts", timeouts.get(),
                "payloadsTooLarge", payloadsTooLarge.get(),
                "bypassed", bypassed.get(),
                "meanFollowerWaitMillis", waits == 0 ? 0 : followerWaitMillisTotal.get() / waits);
    }

    public void reset() {
        leaderExecutions.set(0);
        cacheHits.set(0);
        followerWaits.set(0);
        backgroundRefreshes.set(0);
        failedRetries.set(0);
        leaderTakeovers.set(0);
        timeouts.set(0);
        payloadsTooLarge.set(0);
        bypassed.set(0);
        followerWaitMillisTotal.set(0);
    }
}
