package net.bitsar.coalesce.demo.integration;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import net.bitsar.coalesce.toggle.CoalesceToggle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonReactiveClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The switch is Redis-backed rather than a field, so that one call moves every pod instead
 * of only the one a load balancer happened to route to. These tests exercise it against a
 * real Redis, which is the only place that property is actually observable.
 */
@SpringBootTest
class CoalesceToggleIntegrationTest extends RedisAvailable {

    private static final Duration LIMIT = Duration.ofSeconds(30);

    @Autowired
    CoalesceToggle toggle;

    @Autowired
    RedissonReactiveClient redisson;

    private final String namespace = "Probe.method(" + UUID.randomUUID() + ")";

    @AfterEach
    void leaveTheSwitchAsItWasFound() {
        toggle.setActive(true).block(LIMIT);
        toggle.clearOverride(namespace).block(LIMIT);
    }

    /** Nothing is written at startup, so an unconfigured switch follows coalesce.active. */
    @Test
    void anUnsetSwitchFallsBackToTheConfiguredDefault() {
        assertThat(toggle.isActive().block(LIMIT)).isTrue();
        assertThat(toggle.isActive(namespace).block(LIMIT)).isTrue();
    }

    /**
     * The state lives in Redis, so a second instance built over the same connection sees a
     * flip made through the first. That is the whole point: in a real deployment those two
     * instances are two pods.
     */
    @Test
    void aFlipIsVisibleToAnotherInstanceSharingTheSameRedis() {
        CoalesceToggle otherPod = new net.bitsar.coalesce.toggle.RedisCoalesceToggle(
                redisson, "coalesce:toggle", true);

        toggle.setActive(false).block(LIMIT);

        assertThat(otherPod.isActive().block(LIMIT)).isFalse();
        assertThat(otherPod.isActive(namespace).block(LIMIT)).isFalse();
    }

    @Test
    void oneNamespaceCanBeOverriddenWithoutTouchingTheRest() {
        toggle.setActive(namespace, false).block(LIMIT);

        assertThat(toggle.isActive(namespace).block(LIMIT)).isFalse();
        assertThat(toggle.isActive("some.other.Namespace(int)").block(LIMIT)).isTrue();
        assertThat(toggle.isActive().block(LIMIT)).isTrue();
    }

    /** The big red button cannot be undermined by a stale per-method override. */
    @Test
    void theGlobalSwitchWinsOverAnyOverride() {
        toggle.setActive(namespace, true).block(LIMIT);
        toggle.setActive(false).block(LIMIT);

        assertThat(toggle.isActive(namespace).block(LIMIT)).isFalse();
    }

    @Test
    void setActiveReportsThePositionItReplaced() {
        assertThat(toggle.setActive(false).block(LIMIT)).isTrue();
        assertThat(toggle.setActive(true).block(LIMIT)).isFalse();

        // With no override of its own, a namespace's previous position is the global one.
        assertThat(toggle.setActive(namespace, false).block(LIMIT)).isTrue();
        assertThat(toggle.setActive(namespace, true).block(LIMIT)).isFalse();
    }

    @Test
    void clearingAnOverrideReturnsTheNamespaceToTheGlobalSwitch() {
        toggle.setActive(namespace, false).block(LIMIT);

        assertThat(toggle.clearOverride(namespace).block(LIMIT)).isTrue();
        assertThat(toggle.isActive(namespace).block(LIMIT)).isTrue();
        // Clearing one that was never set is a no-op, not an error.
        assertThat(toggle.clearOverride(namespace).block(LIMIT)).isFalse();
    }

    @Test
    void overridesAreReportedWithoutTheReservedGlobalField() {
        toggle.setActive(false).block(LIMIT);
        toggle.setActive(namespace, false).block(LIMIT);

        Map<String, Boolean> overrides = toggle.overrides().block(LIMIT);

        assertThat(overrides).containsEntry(namespace, false);
        assertThat(overrides).doesNotContainKey("(global)");
    }
}
