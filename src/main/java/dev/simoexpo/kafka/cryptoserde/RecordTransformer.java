package dev.simoexpo.kafka.cryptoserde;

import java.util.function.Function;

public class RecordTransformer<T> {

    private final Function<T, String> idExtractor;
    private final Function<FieldTransformer, Function<T, T>> transformer;

    public RecordTransformer(Function<T, String> idExtractor, Function<FieldTransformer, Function<T, T>> transformer) {
        this.idExtractor = idExtractor;
        this.transformer = transformer;
    }

    public T transform(T input, FieldTransformer fieldTransformer) {
        return transformer.apply(fieldTransformer).apply(input);
    }

    public String extractId(T input) {
        return idExtractor.apply(input);
    }
}
