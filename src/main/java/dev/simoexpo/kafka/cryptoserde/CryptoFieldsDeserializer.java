package dev.simoexpo.kafka.cryptoserde;

import dev.simoexpo.kafka.cryptoserde.secrets.SecretsCache;
import dev.simoexpo.kafka.cryptoserde.utils.CryptoUtils;
import dev.simoexpo.kafka.cryptoserde.secrets.SecretProvider;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.security.GeneralSecurityException;
import java.util.Optional;

public class CryptoFieldsDeserializer<T> implements Deserializer<T> {

    private final Deserializer<T> innerDeserializer;
    private final RecordTransformer<T> transformer;
    private final SecretsCache secretsCache;

    public CryptoFieldsDeserializer(Deserializer<T> deserializer,
                                    RecordTransformer<T> transformer,
                                    SecretProvider secretProvider) {
        this.innerDeserializer = deserializer;
        this.transformer = transformer;
        this.secretsCache = new SecretsCache(secretProvider);
    }

    public static <T1 extends CryptableFields<T1>> CryptoFieldsDeserializer<T1> deriveFrom(Deserializer<T1> deserializer,
                                                                                           SecretProvider secretProvider) {
        return new CryptoFieldsDeserializer<T1>(deserializer,
                new RecordTransformer<T1>(CryptableFields::cryptoId, f -> t -> t.transform(f)),
                secretProvider);
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        T anonymousT = innerDeserializer.deserialize(s, bytes);
        String id = transformer.extractId(anonymousT);
        Optional<T> result = secretsCache.getSecret(id).map(secret -> transformer.transform(anonymousT, fieldEncryptor(secret)));
        return result.orElse(null);
    }

    private static FieldTransformer fieldEncryptor(String secret) {
        return FieldTransformer.defaultAnonymizer(bytes -> {
            try {
                return CryptoUtils.decrypt(bytes, secret);
            } catch (GeneralSecurityException e) {
                throw new SerializationException("Failed to de-anonymize record during deserialization", e);
            }
        });
    }
}
