package dev.simoexpo.kafka.cryptoserde.secrets;

import java.util.Optional;

public interface SecretProvider {

    Optional<String> getSecret(String key);

}
