package net.bitsar.coalesce.autoconfigure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.redisson.config.ReadMode;
import org.redisson.config.SslProvider;
import org.redisson.config.SslVerificationMode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.Resource;

/**
 * Everything under the {@code coalesce.*} prefix.
 *
 * <p>Per-call behaviour — TTLs, timeouts, the key expression — lives on the
 * {@link net.bitsar.coalesce.annotation.Coalesce} annotation instead, because those are
 * properties of the method being coalesced rather than of the application.
 */
@ConfigurationProperties(prefix = "coalesce")
public class CoalesceProperties {

    /** Whether call coalescing is wired at all. Turn off to run without touching Redis. */
    private boolean enabled = true;

    /**
     * The starting position of the runtime kill switch. False starts the application with
     * coalescing bypassed: annotated methods call straight through and nothing touches
     * Redis, but the beans exist, so it can be switched back on without a restart.
     *
     * <p>Not the same as {@code enabled}, which decides whether any of this is wired at
     * all. With {@code enabled=false} there is nothing to switch.
     */
    private boolean active = true;

    /** Base delay between follower polls. A 60% proportional jitter is added to this value. */
    private Duration pollInterval = Duration.ofMillis(200);

    /**
     * Results larger than this are still returned to the caller but never written to Redis.
     * Redisson buffers every command in Netty's direct arena before the write, so a handful
     * of oversized entries in flight can exhaust MaxDirectMemorySize and kill the process —
     * the payload never reaches Redis, it dies in the encoder.
     */
    private int maxPayloadBytes = 1_048_576;

    private final Redis redis = new Redis();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    public int getMaxPayloadBytes() {
        return maxPayloadBytes;
    }

    public void setMaxPayloadBytes(int maxPayloadBytes) {
        this.maxPayloadBytes = maxPayloadBytes;
    }

    public Redis getRedis() {
        return redis;
    }

    /** Which Redis topology to build a client for. */
    public enum Mode {

        /** One Redis server, addressed by {@code coalesce.redis.address}. */
        SINGLE,

        /** Redis Cluster, addressed by {@code coalesce.redis.nodes}. */
        CLUSTER
    }

    /**
     * Connection settings for the {@code RedissonClient} this library creates when the
     * application has not defined one. Ignored entirely if it has: declare your own
     * {@code RedissonClient} or {@code RedissonReactiveClient} bean for sentinel,
     * replicated, or any topology beyond the two modes here, and it is used as-is.
     */
    public static class Redis {

        /** Set to false to require the application to supply its own Redisson client. */
        private boolean enabled = true;

        /** Which topology to build. {@code CLUSTER} reads {@link #nodes} instead of {@link #address}. */
        private Mode mode = Mode.SINGLE;

        // ---------- SINGLE ----------

        /**
         * Single-server address, including the scheme: {@code redis://} or {@code rediss://}.
         * Ignored when {@code mode} is {@code CLUSTER}.
         */
        private String address = "redis://localhost:6379";

        /**
         * Redis database index. Ignored by Redis Cluster, which only has database 0 — setting
         * it in {@code CLUSTER} mode logs a warning and is dropped.
         */
        private int database = 0;

        private int connectionPoolSize = 64;

        private int connectionMinimumIdleSize = 10;

        // ---------- CLUSTER ----------

        /**
         * Cluster node addresses, each with a scheme. Only seed nodes are needed — Redisson
         * discovers the rest of the topology from them. Required when {@code mode} is
         * {@code CLUSTER}.
         */
        private List<String> nodes = new ArrayList<>();

        /**
         * Where cluster reads are served from. Leave at {@code MASTER}.
         *
         * <p>Reading from a replica breaks coalescing rather than merely slowing it: a
         * follower polling for the leader's outcome can be served a stale {@code FAILED} —
         * or a stale absence — for work that has already succeeded, and will re-execute it.
         * The library warns loudly if this is set to anything else.
         */
        private ReadMode readMode = ReadMode.MASTER;

        /** How often Redisson re-scans the cluster for topology changes. */
        private Duration scanInterval = Duration.ofSeconds(5);

        private int masterConnectionPoolSize = 64;

        private int masterConnectionMinimumIdleSize = 10;

        private int slaveConnectionPoolSize = 64;

        private int slaveConnectionMinimumIdleSize = 10;

        // ---------- both modes ----------

        /** Redis username, for ACL-enabled servers. Empty means "default user". */
        private String username = "";

        /** Redis password, or empty for an unauthenticated server. */
        private String password = "";

        /** How long a command may wait for a response before Redisson fails it. */
        private Duration timeout = Duration.ofSeconds(3);

        /** How long to wait for a connection to be established. */
        private Duration connectTimeout = Duration.ofSeconds(10);

        private final Ssl ssl = new Ssl();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Mode getMode() {
            return mode;
        }

        public void setMode(Mode mode) {
            this.mode = mode;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public int getDatabase() {
            return database;
        }

        public void setDatabase(int database) {
            this.database = database;
        }

        public int getConnectionPoolSize() {
            return connectionPoolSize;
        }

        public void setConnectionPoolSize(int connectionPoolSize) {
            this.connectionPoolSize = connectionPoolSize;
        }

        public int getConnectionMinimumIdleSize() {
            return connectionMinimumIdleSize;
        }

        public void setConnectionMinimumIdleSize(int connectionMinimumIdleSize) {
            this.connectionMinimumIdleSize = connectionMinimumIdleSize;
        }

        public List<String> getNodes() {
            return nodes;
        }

        public void setNodes(List<String> nodes) {
            this.nodes = nodes;
        }

        public ReadMode getReadMode() {
            return readMode;
        }

        public void setReadMode(ReadMode readMode) {
            this.readMode = readMode;
        }

        public Duration getScanInterval() {
            return scanInterval;
        }

        public void setScanInterval(Duration scanInterval) {
            this.scanInterval = scanInterval;
        }

        public int getMasterConnectionPoolSize() {
            return masterConnectionPoolSize;
        }

        public void setMasterConnectionPoolSize(int masterConnectionPoolSize) {
            this.masterConnectionPoolSize = masterConnectionPoolSize;
        }

        public int getMasterConnectionMinimumIdleSize() {
            return masterConnectionMinimumIdleSize;
        }

        public void setMasterConnectionMinimumIdleSize(int masterConnectionMinimumIdleSize) {
            this.masterConnectionMinimumIdleSize = masterConnectionMinimumIdleSize;
        }

        public int getSlaveConnectionPoolSize() {
            return slaveConnectionPoolSize;
        }

        public void setSlaveConnectionPoolSize(int slaveConnectionPoolSize) {
            this.slaveConnectionPoolSize = slaveConnectionPoolSize;
        }

        public int getSlaveConnectionMinimumIdleSize() {
            return slaveConnectionMinimumIdleSize;
        }

        public void setSlaveConnectionMinimumIdleSize(int slaveConnectionMinimumIdleSize) {
            this.slaveConnectionMinimumIdleSize = slaveConnectionMinimumIdleSize;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public Duration getTimeout() {
            return timeout;
        }

        public void setTimeout(Duration timeout) {
            this.timeout = timeout;
        }

        public Duration getConnectTimeout() {
            return connectTimeout;
        }

        public void setConnectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
        }

        public Ssl getSsl() {
            return ssl;
        }
    }

    /**
     * TLS settings, applied to whichever topology is in use.
     *
     * <p>Redisson decides whether a connection is encrypted from the address scheme, not
     * from a flag, which is a reliable way to configure a truststore and still connect in
     * plaintext. Setting {@code enabled} here rewrites {@code redis://} addresses to
     * {@code rediss://} so the two cannot disagree.
     */
    public static class Ssl {

        /**
         * Rewrites every {@code redis://} address to {@code rediss://}. Addresses already
         * written as {@code rediss://} are encrypted whether or not this is set.
         */
        private boolean enabled = false;

        /**
         * Truststore holding the CA that signed the server's certificate. Any Spring
         * resource location: {@code classpath:}, {@code file:}, or a bare path. Leave unset
         * to use the JVM's default truststore.
         */
        private Resource truststore;

        private String truststorePassword = "";

        /** Client certificate keystore, for servers that require mutual TLS. */
        private Resource keystore;

        private String keystorePassword = "";

        /** Keystore format, e.g. {@code JKS} or {@code PKCS12}. Null uses the JVM default. */
        private String keystoreType;

        /**
         * How far to verify the server's certificate. {@code STRICT} checks the chain and
         * the hostname. {@code CA_ONLY} skips the hostname check, which is what a managed
         * Redis addressed by IP usually needs. {@code NONE} disables verification and makes
         * the connection trivially interceptable — do not ship it.
         */
        private SslVerificationMode verificationMode = SslVerificationMode.STRICT;

        /** TLS versions to allow, e.g. {@code TLSv1.3}. Empty uses the JVM default. */
        private List<String> protocols = new ArrayList<>();

        /** Cipher suites to allow. Empty uses the JVM default. */
        private List<String> ciphers = new ArrayList<>();

        /** {@code JDK} or {@code OPENSSL}. OpenSSL is faster but needs a netty-tcnative jar. */
        private SslProvider provider = SslProvider.JDK;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Resource getTruststore() {
            return truststore;
        }

        public void setTruststore(Resource truststore) {
            this.truststore = truststore;
        }

        public String getTruststorePassword() {
            return truststorePassword;
        }

        public void setTruststorePassword(String truststorePassword) {
            this.truststorePassword = truststorePassword;
        }

        public Resource getKeystore() {
            return keystore;
        }

        public void setKeystore(Resource keystore) {
            this.keystore = keystore;
        }

        public String getKeystorePassword() {
            return keystorePassword;
        }

        public void setKeystorePassword(String keystorePassword) {
            this.keystorePassword = keystorePassword;
        }

        public String getKeystoreType() {
            return keystoreType;
        }

        public void setKeystoreType(String keystoreType) {
            this.keystoreType = keystoreType;
        }

        public SslVerificationMode getVerificationMode() {
            return verificationMode;
        }

        public void setVerificationMode(SslVerificationMode verificationMode) {
            this.verificationMode = verificationMode;
        }

        public List<String> getProtocols() {
            return protocols;
        }

        public void setProtocols(List<String> protocols) {
            this.protocols = protocols;
        }

        public List<String> getCiphers() {
            return ciphers;
        }

        public void setCiphers(List<String> ciphers) {
            this.ciphers = ciphers;
        }

        public SslProvider getProvider() {
            return provider;
        }

        public void setProvider(SslProvider provider) {
            this.provider = provider;
        }
    }
}
