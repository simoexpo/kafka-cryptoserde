package dev.simoexpo.kafka.cryptoserde;

import dev.simoexpo.kafka.cryptoserde.secrets.SecretProvider;
import dev.simoexpo.kafka.cryptoserde.secrets.SecretsCache;
import dev.simoexpo.kafka.cryptoserde.utils.CryptoUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;

public class CryptoRecordDeserializer<T> implements Deserializer<T> {

    private static final String SEPARATOR = "#";
    private final Deserializer<T> innerDeserializer;
    private final SecretProvider secretProvider;

    public CryptoRecordDeserializer(Deserializer<T> deserializer,
                                    SecretProvider secretProvider) {
        this.innerDeserializer = deserializer;
        this.secretProvider = secretProvider;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        String data = new String(bytes, StandardCharsets.UTF_8);
        String[] split = data.split(SEPARATOR);
        if (split.length != 3) {
            return null;
        } else {
            Base64.Decoder decoder = Base64.getDecoder();
            String id = new String(decoder.decode(split[0]), StandardCharsets.UTF_8);
            return secretProvider.getSecret(id).map(secret -> {
                try {
                    byte[] recordData = (split[1] + SEPARATOR + split[2]).getBytes(StandardCharsets.UTF_8);
                    return innerDeserializer.deserialize(s, CryptoUtils.decrypt(recordData, secret));
                } catch (GeneralSecurityException e) {
                    throw new SerializationException("Failed to de-anonymize record during deserialization", e);
                }
            }).orElse(null);
        }

    }

}
