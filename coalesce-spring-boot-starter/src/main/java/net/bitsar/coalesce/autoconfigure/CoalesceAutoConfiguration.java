package net.bitsar.coalesce.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import net.bitsar.coalesce.annotation.CoalesceAttributeResolver;
import net.bitsar.coalesce.aspect.CoalesceAspect;
import net.bitsar.coalesce.aspect.CoalesceKeyResolver;
import net.bitsar.coalesce.codec.CoalesceCodec;
import net.bitsar.coalesce.codec.JsonCoalesceCodec;
import net.bitsar.coalesce.coordinator.CoalesceCoordinator;
import net.bitsar.coalesce.coordinator.RedissonCoalesceCoordinator;
import net.bitsar.coalesce.metrics.CoalesceMetrics;
import net.bitsar.coalesce.core.CoalesceKeys;
import net.bitsar.coalesce.toggle.CoalesceToggle;
import net.bitsar.coalesce.toggle.RedisCoalesceToggle;
import org.aspectj.lang.annotation.Aspect;
import org.redisson.api.RedissonReactiveClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Mono;

/**
 * Wires {@code @Coalesce} without asking the application to component-scan this library.
 *
 * <p>Every bean here backs off from an application-declared one of the same type, so any
 * single piece can be replaced: a {@link CoalesceCodec} for a different wire format, a
 * {@link CoalesceCoordinator} for a different backing store, a {@link CoalesceKeyResolver}
 * for a different key convention.
 *
 * <p>Ordered after {@link CoalesceRedissonAutoConfiguration} because the coordinator's
 * {@code @ConditionalOnBean(RedissonReactiveClient.class)} can only see a client that has
 * already been contributed.
 */
@AutoConfiguration(after = CoalesceRedissonAutoConfiguration.class)
@ConditionalOnClass({RedissonReactiveClient.class, Aspect.class, Mono.class})
@ConditionalOnProperty(prefix = "coalesce", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(CoalesceProperties.class)
public class CoalesceAutoConfiguration {

    /**
     * @return the leader/hit/wait counters, which are the only way to tell whether
     *         coalescing is actually happening
     */
    @Bean
    @ConditionalOnMissingBean
    public CoalesceMetrics coalesceMetrics() {
        return new CoalesceMetrics();
    }

    /**
     * Resolves {@code ${...}} in annotation attributes against the environment, which is
     * what lets a TTL come from a property or an environment variable rather than being
     * frozen at compile time.
     *
     * @param beanFactory supplies the same placeholder resolution {@code @Value} uses
     * @return the attribute resolver
     */
    @Bean
    @ConditionalOnMissingBean
    public CoalesceAttributeResolver coalesceAttributeResolver(ConfigurableBeanFactory beanFactory) {
        return new CoalesceAttributeResolver(beanFactory::resolveEmbeddedValue);
    }

    /**
     * @return the SpEL-based resolver turning a method invocation into its Redis key
     */
    @Bean
    @ConditionalOnMissingBean
    public CoalesceKeyResolver coalesceKeyResolver() {
        return new CoalesceKeyResolver();
    }

    /**
     * Prefers the application's {@code ObjectMapper} so cached payloads serialise exactly
     * the way its HTTP responses do. The fallback registers whatever Jackson modules are on
     * the classpath — without at least JSR-310, a DTO carrying an {@code Instant} encodes
     * fine and then fails to decode.
     *
     * @param objectMapper the context's mapper, if it has one
     * @return the payload codec
     */
    @Bean
    @ConditionalOnMissingBean(CoalesceCodec.class)
    public JsonCoalesceCodec coalesceCodec(ObjectProvider<ObjectMapper> objectMapper) {
        return new JsonCoalesceCodec(
                objectMapper.getIfAvailable(() -> JsonMapper.builder().findAndAddModules().build()));
    }

    /**
     * @param redisson the reactive client from {@link CoalesceRedissonAutoConfiguration} or
     *                 from the application
     * @return the lock/bucket/topic coordinator
     */
    @Bean
    @ConditionalOnBean(RedissonReactiveClient.class)
    @ConditionalOnMissingBean(CoalesceCoordinator.class)
    public RedissonCoalesceCoordinator coalesceCoordinator(RedissonReactiveClient redisson) {
        return new RedissonCoalesceCoordinator(redisson);
    }

    /**
     * The around-advice itself. Requires a coordinator, so it is absent — rather than
     * broken — on a context with no Redis client at all.
     *
     * @param coordinator shared state across pods
     * @param codec       payload wire format
     * @param metrics     counters
     * @param keyResolver       key derivation
     * @param attributeResolver placeholder resolution for annotation attributes
     * @param properties        the {@code coalesce.*} settings
     * @return the aspect that intercepts {@code @Coalesce} methods
     */
    /**
     * The runtime kill switch, held in Redis so one call moves the whole fleet rather than
     * the single pod a load balancer happened to route to. Declare your own bean to drive
     * it from somewhere else, such as a feature-flag service.
     */
    @Bean
    @ConditionalOnBean(RedissonReactiveClient.class)
    @ConditionalOnMissingBean
    public CoalesceToggle coalesceToggle(RedissonReactiveClient redisson, CoalesceProperties properties) {
        return new RedisCoalesceToggle(redisson, CoalesceKeys.KEY_PREFIX + "toggle", properties.isActive());
    }

    @Bean
    @ConditionalOnBean(CoalesceCoordinator.class)
    @ConditionalOnMissingBean
    public CoalesceAspect coalesceAspect(CoalesceCoordinator coordinator,
                                         CoalesceCodec codec,
                                         CoalesceMetrics metrics,
                                         CoalesceKeyResolver keyResolver,
                                         CoalesceAttributeResolver attributeResolver,
                                         CoalesceToggle toggle,
                                         CoalesceProperties properties) {
        return new CoalesceAspect(coordinator, codec, metrics, keyResolver, attributeResolver,
                toggle, properties.getMaxPayloadBytes(), properties.getPollInterval());
    }
}
