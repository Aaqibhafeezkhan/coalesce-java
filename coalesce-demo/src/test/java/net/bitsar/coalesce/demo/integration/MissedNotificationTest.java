package net.bitsar.coalesce.demo.integration;

import net.bitsar.coalesce.coordinator.CoalesceCoordinator;
import net.bitsar.coalesce.coordinator.RedissonCoalesceCoordinator;
import net.bitsar.coalesce.demo.DemoMetrics;
import net.bitsar.coalesce.demo.Mode;
import net.bitsar.coalesce.demo.OrderDto;
import net.bitsar.coalesce.demo.OrderService;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonReactiveClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.TestPropertySource;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pub/sub is fire-and-forget: a wake-up can be dropped. With the topic listener stubbed out
 * entirely, the jittered poll in the follower wait loop has to resolve the wait on its own.
 */
@SpringBootTest
@TestPropertySource(properties = {
        "demo.latency.mean-millis=120",
        "demo.latency.std-dev-millis=0",
        "demo.latency.max-millis=200"
})
class MissedNotificationTest extends RedisAvailable {

    @Autowired
    OrderService orders;

    @Autowired
    DemoMetrics demoMetrics;

    @Autowired
    RedissonReactiveClient redisson;

    @Test
    void followersStillResolveWhenEveryWakeUpIsLost() {
        demoMetrics.reset();
        redisson.getKeys().deleteByPattern("coalesce:*").block(Duration.ofSeconds(10));
        int bucket = 9;

        long startedAt = System.currentTimeMillis();
        List<List<OrderDto>> results = Flux.range(0, 8)
                .flatMap(i -> orders.loadCoalesced(bucket))
                .collectList()
                .block(Duration.ofSeconds(30));
        long elapsed = System.currentTimeMillis() - startedAt;

        assertThat(results).hasSize(8);
        assertThat(demoMetrics.snapshot(Mode.COALESCED).get("downstreamExecutions")).isEqualTo(1L);
        assertThat(results).allSatisfy(r -> assertThat(r).isEqualTo(results.get(0)));

        // Polling adds at most an interval or two on top of the ~120ms downstream. The point
        // is that it resolves well inside waitTimeoutSeconds rather than hanging.
        assertThat(elapsed).isLessThan(3_000);
    }

    @TestConfiguration
    static class DeafCoordinatorConfig {

        @Bean
        @Primary
        CoalesceCoordinator deafCoordinator(RedissonReactiveClient redisson) {
            return new RedissonCoalesceCoordinator(redisson) {
                @Override
                public Flux<String> listen(String key) {
                    return Flux.never(); // every notification is lost
                }
            };
        }
    }
}
