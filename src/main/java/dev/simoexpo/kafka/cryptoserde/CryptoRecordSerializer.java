package dev.simoexpo.kafka.cryptoserde;

import dev.simoexpo.kafka.cryptoserde.secrets.SecretProvider;
import dev.simoexpo.kafka.cryptoserde.secrets.SecretsCache;
import dev.simoexpo.kafka.cryptoserde.utils.CryptoUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.function.Function;
import java.util.stream.Stream;

public class CryptoRecordSerializer<T> implements Serializer<T> {

    private final Serializer<T> innerSerializer;
    private final Function<T, String> idExtractor;
    private final SecretsCache secretsCache;

    public CryptoRecordSerializer(Serializer<T> serializer,
                                  Function<T, String> idExtractor,
                                  SecretProvider secretProvider) {
        this.innerSerializer = serializer;
        this.idExtractor = idExtractor;
        this.secretsCache = new SecretsCache(secretProvider);

    }

    @Override
    public byte[] serialize(String s, T t) {
        String id = idExtractor.apply(t);
        return secretsCache.getSecret(id).map(secret -> {
            try {
                Base64.Encoder encoder = Base64.getEncoder();
                byte[] id64 = encoder.encode(id.getBytes(StandardCharsets.UTF_8));
                byte[] separator = "#".getBytes(StandardCharsets.UTF_8);
                byte[] encrypted = CryptoUtils.encrypt(innerSerializer.serialize(s, t), secret);
                ByteBuffer bb = ByteBuffer.allocate(id64.length + separator.length + encrypted.length);
                Stream.of(id64, separator, encrypted).forEach(bb::put);
                return bb.array();
            } catch (GeneralSecurityException e) {
                throw new SerializationException("Failed to anonymize record during serialization", e);
            }
        }).orElse(null);
    }
}
