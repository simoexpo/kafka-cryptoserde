package dev.simoexpo.kafka.cryptoserde;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

public interface FieldTransformer {

    byte[] transform(byte[] field);

    String transform(String field);

    static FieldTransformer defaultAnonymizer(Function<byte[], byte[]> transformer) {
        return new FieldTransformer() {
            @Override
            public byte[] transform(byte[] field) {
                return transformer.apply(field);
            }

            @Override
            public String transform(String field) {
                return new String(transformer.apply(field.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
            }
        };
    }
}
