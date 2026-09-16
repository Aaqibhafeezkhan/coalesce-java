package net.bitsar.coalesce.aspect;

import java.lang.reflect.Method;
import java.time.Duration;
import net.bitsar.coalesce.annotation.CoalesceAttributeResolver;
import net.bitsar.coalesce.autoconfigure.CoalesceProperties;
import net.bitsar.coalesce.codec.CoalesceCodec;
import net.bitsar.coalesce.coordinator.CoalesceCoordinator;
import net.bitsar.coalesce.metrics.CoalesceMetrics;
import net.bitsar.coalesce.toggle.CoalesceToggle;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class CoalescePollIntervalTest {

    private final CoalesceCoordinator coordinator = mock(CoalesceCoordinator.class);
    private final CoalesceCodec codec = mock(CoalesceCodec.class);
    private final CoalesceMetrics metrics = new CoalesceMetrics();
    private final CoalesceKeyResolver keyResolver = mock(CoalesceKeyResolver.class);
    private final CoalesceAttributeResolver attributeResolver = mock(CoalesceAttributeResolver.class);
    private final CoalesceToggle toggle = mock(CoalesceToggle.class);

    @Test
    void configuredPollIntervalIsUsedAsTheBaseDelay() throws Exception {
        CoalesceAspect aspect = newAspect(Duration.ofMillis(500));
        Method pollDelay = CoalesceAspect.class.getDeclaredMethod("pollDelay");
        pollDelay.setAccessible(true);

        for (int i = 0; i < 100; i++) {
            long delay = ((Duration) pollDelay.invoke(aspect)).toMillis();
            assertThat(delay).isBetween(500L, 799L);
        }
    }

    @Test
    void defaultPollIntervalPreservesTheExistingBaseDelay() throws Exception {
        CoalesceProperties properties = new CoalesceProperties();
        CoalesceAspect aspect = newAspect(properties.getPollInterval());
        Method pollDelay = CoalesceAspect.class.getDeclaredMethod("pollDelay");
        pollDelay.setAccessible(true);

        long delay = ((Duration) pollDelay.invoke(aspect)).toMillis();
        assertThat(delay).isBetween(200L, 319L);
    }

    private CoalesceAspect newAspect(Duration pollInterval) {
        return new CoalesceAspect(coordinator, codec, metrics, keyResolver, attributeResolver,
                toggle, 1_048_576, pollInterval);
    }
}
