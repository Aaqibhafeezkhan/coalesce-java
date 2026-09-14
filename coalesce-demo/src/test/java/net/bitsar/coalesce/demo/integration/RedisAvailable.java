package net.bitsar.coalesce.demo.integration;

import java.net.InetSocketAddress;
import java.net.Socket;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/** Integration tests need a real Redis; without one they skip rather than fail the build. */
abstract class RedisAvailable {

    private static final int REDIS_PORT = 6379;
    private static final GenericContainer<?> REDIS_CONTAINER = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(REDIS_PORT);
    private static volatile String address;
    private static volatile IllegalStateException startupFailure;

    static boolean check() {
        ensureStarted();
        return true;
    }

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("coalesce.redis.address", RedisAvailable::address);
    }

    private static String address() {
        ensureStarted();
        return address;
    }

    private static void ensureStarted() {
        if (startupFailure != null) {
            throw startupFailure;
        }
        if (address != null) {
            return;
        }

        synchronized (RedisAvailable.class) {
            if (startupFailure != null) {
                throw startupFailure;
            }
            if (address != null) {
                return;
            }

            if (isReachable("localhost", REDIS_PORT)) {
                address = "redis://localhost:" + REDIS_PORT;
                return;
            }

            try {
                REDIS_CONTAINER.start();
                address = "redis://" + REDIS_CONTAINER.getHost() + ":"
                        + REDIS_CONTAINER.getMappedPort(REDIS_PORT);
            } catch (Exception e) {
                startupFailure = new IllegalStateException(
                        "Redis integration tests require Redis on localhost:6379 or Docker "
                                + "for Testcontainers. No Redis was reachable and "
                                + "Testcontainers could not start redis:7-alpine.", e);
                throw startupFailure;
            }
        }
    }

    private static boolean isReachable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 500);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
