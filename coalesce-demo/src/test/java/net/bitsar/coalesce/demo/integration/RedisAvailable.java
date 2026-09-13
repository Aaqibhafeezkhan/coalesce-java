package net.bitsar.coalesce.demo.integration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.net.Socket;

final class RedisAvailable {

    private static final int REDIS_PORT = 6379;
    private static final String REDIS_IMAGE = "redis:7-alpine";
    private static GenericContainer<?> container;

    private RedisAvailable() {
    }

    static synchronized boolean check() {
        if (isReachable()) {
            System.setProperty("coalesce.redis.address", "redis://localhost:6379");
            return true;
        }

        if (container != null && container.isRunning()) {
            configureContainerAddress();
            return true;
        }

        try {
            container = new GenericContainer<>(DockerImageName.parse(REDIS_IMAGE))
                    .withExposedPorts(REDIS_PORT)
                    .waitingFor(Wait.forListeningPort());
            container.start();
            configureContainerAddress();
            Runtime.getRuntime().addShutdownHook(new Thread(container::stop));
            return true;
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "Redis is not reachable on localhost:6379 and Testcontainers could not start "
                            + REDIS_IMAGE + ". Start Redis locally or make Docker available before "
                            + "running the integration tests.",
                    e);
        }
    }

    private static boolean isReachable() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", REDIS_PORT), 500);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void configureContainerAddress() {
        System.setProperty(
                "coalesce.redis.address",
                "redis://" + container.getHost() + ":" + container.getMappedPort(REDIS_PORT));
    }
}
