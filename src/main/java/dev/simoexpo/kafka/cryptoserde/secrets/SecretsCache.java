package dev.simoexpo.kafka.cryptoserde.secrets;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import java.time.Duration;
import java.util.Optional;

public class SecretsCache {

    public static final long MAXIMUM_SIZE = 10_000;
    public static final long TTL = 5 * 60 * 1000;
    protected final SecretProvider secretProvider;
    private final LoadingCache<String, String> secretsCache;

    public SecretsCache(SecretProvider secretProvider, long ttl, long maxSize) {
        this.secretProvider = secretProvider;
        this.secretsCache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(Duration.ofMillis(ttl))
                .build(key -> secretProvider.getSecret(key).orElse(null));
    }

    public SecretsCache(SecretProvider secretProvider) {
        this(secretProvider, TTL, MAXIMUM_SIZE);
    }

    public Optional<String> getSecret(String id) {
        return Optional.ofNullable(secretsCache.get(id));
    }
}
