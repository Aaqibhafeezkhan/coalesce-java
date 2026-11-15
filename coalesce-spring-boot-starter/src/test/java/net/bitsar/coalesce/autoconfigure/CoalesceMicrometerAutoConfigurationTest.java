package net.bitsar.coalesce.autoconfigure;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.bitsar.coalesce.metrics.CoalesceMetrics;
import net.bitsar.coalesce.metrics.MicrometerCoalesceMetrics;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class CoalesceMicrometerAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    CoalesceAutoConfiguration.class,
                    CoalesceMicrometerAutoConfiguration.class));

    @Test
    void usesMicrometerMetricsWhenRegistryIsAvailable() {
        runner.withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .run(context -> assertThat(context)
                        .hasSingleBean(CoalesceMetrics.class)
                        .hasSingleBean(MicrometerCoalesceMetrics.class));
    }

    @Test
    void keepsTheInMemoryImplementationWhenMicrometerRegistryIsAbsent() {
        runner.run(context -> assertThat(context)
                .hasSingleBean(CoalesceMetrics.class)
                .doesNotHaveBean(MicrometerCoalesceMetrics.class));
    }

    @Test
    void backsOffWhenTheApplicationProvidesItsOwnMetrics() {
        CoalesceMetrics custom = new CoalesceMetrics();
        runner.withBean(CoalesceMetrics.class, () -> custom)
                .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .run(context -> assertThat(context.getBean(CoalesceMetrics.class)).isSameAs(custom)
                        .doesNotHaveBean(MicrometerCoalesceMetrics.class));
    }
}
