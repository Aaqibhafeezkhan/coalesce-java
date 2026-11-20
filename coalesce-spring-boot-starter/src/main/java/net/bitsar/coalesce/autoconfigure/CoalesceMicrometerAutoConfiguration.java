package net.bitsar.coalesce.autoconfigure;

import io.micrometer.core.instrument.MeterRegistry;
import net.bitsar.coalesce.metrics.CoalesceMetrics;
import net.bitsar.coalesce.metrics.MicrometerCoalesceMetrics;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Replaces the in-memory counters with namespace-tagged Micrometer meters when an
 * application provides a MeterRegistry. Micrometer remains an optional dependency.
 */
@AutoConfiguration(after = CoalesceAutoConfiguration.class)
public class CoalesceMicrometerAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnBean(MeterRegistry.class)
    static class MicrometerConfiguration {

        @Bean
        @ConditionalOnMissingBean(CoalesceMetrics.class)
        MicrometerCoalesceMetrics coalesceMetrics(MeterRegistry registry) {
            return new MicrometerCoalesceMetrics(registry);
        }
    }
}
