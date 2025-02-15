package dev.simoexpo.kafka.cryptoserde;

import dev.simoexpo.kafka.cryptoserde.secrets.SecretsCache;
import dev.simoexpo.kafka.cryptoserde.utils.CryptoUtils;
import dev.simoexpo.kafka.cryptoserde.secrets.SecretProvider;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.security.*;
import java.util.Optional;

public class CryptoFieldsSerializer<T> implements Serializer<T> {

    private final Serializer<T> innerSerializer;
    private final RecordTransformer<T> transformer;
    private final SecretsCache secretsCache;

    public CryptoFieldsSerializer(Serializer<T> serializer,
                                  RecordTransformer<T> transformer,
                                  SecretProvider secretProvider) {
        this.innerSerializer = serializer;
        this.transformer = transformer;
        this.secretsCache = new SecretsCache(secretProvider);
    }

    public static <T1 extends CryptableFields<T1>> CryptoFieldsSerializer<T1> deriveFrom(Serializer<T1> serializer,
                                                                                         SecretProvider secretProvider) {
        return new CryptoFieldsSerializer<T1>(serializer,
                new RecordTransformer<T1>(CryptableFields::cryptoId, f -> t -> t.transform(f)),
                secretProvider);
    }

    @Override
    public byte[] serialize(String s, T t) {
        String id = transformer.extractId(t);
        Optional<T> anonymousT = secretsCache.getSecret(id).map(secret -> transformer.transform(t, fieldEncryptor(secret)));
        return innerSerializer.serialize(s, anonymousT.orElse(null));
    }

    private static FieldTransformer fieldEncryptor(String secret) {
        return FieldTransformer.defaultAnonymizer(bytes -> {
            try {
                return CryptoUtils.encrypt(bytes, secret);
            } catch (GeneralSecurityException e) {
                throw new SerializationException("Failed to anonymize record during serialization", e);
            }
        });
    }
}
