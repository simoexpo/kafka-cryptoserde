package dev.simoexpo.kafka.cryptoserde.secrets;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SecretsCacheTest {

    @Test
    public void shouldRetrieveSecret() {
        String key = "key";
        String secret = "secret";
        SecretProvider secretProvider = new SecretProvider() {
            @Override
            public Optional<String> getSecret(String key) {
                return Optional.of(secret);
            }
        };
        SecretsCache cache = new SecretsCache(secretProvider) {};
        assertEquals(Optional.of(secret), cache.getSecret(key));
    }

    @Test
    public void shouldRetrieveACachedSecret() {
        String key = "key";
        String secret = "secret";
        SecretProvider secretProvider = new SecretProvider() {
            private boolean delivered = false;

            @Override
            public Optional<String> getSecret(String key) {
                if (delivered) {
                    return Optional.empty();
                } else {
                    delivered = true;
                    return Optional.of(secret);
                }
            }
        };
        SecretsCache cache = new SecretsCache(secretProvider) {};
        assertEquals(Optional.of(secret), cache.getSecret(key));
        assertEquals(Optional.of(secret), cache.getSecret(key));
    }

    @Test
    public void shouldExpireOldSecrets() {
        String key = "key";
        String secret = "secret";
        SecretProvider secretProvider = new SecretProvider() {
            private boolean delivered = false;

            @Override
            public Optional<String> getSecret(String key) {
                if (delivered) {
                    return Optional.empty();
                } else {
                    delivered = true;
                    return Optional.of(secret);
                }
            }
        };
        SecretsCache cache = new SecretsCache(secretProvider, 100, 10) {};
        assertEquals(Optional.of(secret), cache.getSecret(key));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertEquals(Optional.empty(), cache.getSecret(key));
    }
}
