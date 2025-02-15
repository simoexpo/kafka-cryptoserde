package dev.simoexpo.kafka.cryptoserde;

public interface CryptableFields<T> {

    String cryptoId();
    T transform(FieldTransformer fieldTransformer);

}
