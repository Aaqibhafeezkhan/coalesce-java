package net.bitsar.coalesce.aspect;

import java.lang.reflect.Method;
import net.bitsar.coalesce.autoconfigure.CoalesceAutoConfiguration;
import net.bitsar.coalesce.autoconfigure.CoalesceRedissonAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.redisson.api.RedissonReactiveClient;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class CoalescePollIntervalTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    CoalesceRedissonAutoConfiguration.class,
                    CoalesceAutoConfiguration.class))
            .withUserConfiguration(MockRedissonConfig.class);

    @Test
    void configuredPollIntervalIsUsedAsTheBaseDelay() throws Exception {
        runner.withPropertyValues("coalesce.poll-interval=500ms").run(context -> {
            CoalesceAspect aspect = context.getBean(CoalesceAspect.class);
            Method pollDelay = CoalesceAspect.class.getDeclaredMethod("pollDelay");
            pollDelay.setAccessible(true);

            for (int i = 0; i < 100; i++) {
                long delay = ((java.time.Duration) pollDelay.invoke(aspect)).toMillis();
                assertThat(delay).isBetween(500L, 799L);
            }
        });
    }

    @Test
    void defaultPollIntervalPreservesTheExistingBaseDelay() throws Exception {
        runner.run(context -> {
            CoalesceAspect aspect = context.getBean(CoalesceAspect.class);
            Method pollDelay = CoalesceAspect.class.getDeclaredMethod("pollDelay");
            pollDelay.setAccessible(true);

            long delay = ((java.time.Duration) pollDelay.invoke(aspect)).toMillis();
            assertThat(delay).isBetween(200L, 319L);
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class MockRedissonConfig {
        @Bean
        RedissonReactiveClient redissonReactiveClient() {
            return Mockito.mock(RedissonReactiveClient.class);
        }
    }
}
