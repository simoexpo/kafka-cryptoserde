package dev.simoexpo.kafka.cryptoserde;

import dev.simoexpo.kafka.cryptoserde.secrets.SecretProvider;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class CryptoFieldsSerdeTest {

    record TestRecord(String id, String value, int number) implements CryptableFields<TestRecord> {

        @Override
        public String cryptoId() {
            return id;
        }

        @Override
        public TestRecord transform(FieldTransformer fieldTransformer) {
            return new TestRecord(id, fieldTransformer.transform(value), number);
        }
    }

    String topic = "test-topic";
    JsonSerde<TestRecord> serde = new JsonSerde<>(TestRecord.class);
    SecretProvider dummySecretProvider = key -> Optional.of(key.repeat(32 / key.length() + 1).substring(0, 32));

    @Test
    public void shouldSerializeAndDeserialize() {
        TestRecord record = new TestRecord("1", "value", 1);
        Serializer<TestRecord> serializer = CryptoFieldsSerializer.deriveFrom(serde.serializer(), dummySecretProvider);
        Deserializer<TestRecord> deserializer = CryptoFieldsDeserializer.deriveFrom(serde.deserializer(), dummySecretProvider);
        TestRecord result = deserializer.deserialize(topic, serializer.serialize(topic, record));
        assertEquals(record, result);
    }

    @Test
    public void shouldNotSerializeWhenSecretDoesNotExist() {
        TestRecord record = new TestRecord("1", "value", 1);
        Serializer<TestRecord> serializer = CryptoFieldsSerializer.deriveFrom(serde.serializer(), k -> Optional.empty());
        assertNull(serializer.serialize(topic, record));
    }

    @Test
    public void shouldNotDeserializeWhenSecretDoesNotExist() {
        TestRecord record = new TestRecord("1", "value", 1);
        Serializer<TestRecord> serializer = CryptoFieldsSerializer.deriveFrom(serde.serializer(), dummySecretProvider);
        Deserializer<TestRecord> deserializer = CryptoFieldsDeserializer.deriveFrom(serde.deserializer(), k -> Optional.empty());
        TestRecord result = deserializer.deserialize(topic, serializer.serialize(topic, record));
        assertNull(result);
    }

    @Test
    public void shouldNotDeserializeWithStandardDeserializer() {
        TestRecord record = new TestRecord("1", "value", 1);
        RecordTransformer<TestRecord> recordTransformer = new RecordTransformer<>(
                s -> s.id,
                f -> r -> new TestRecord(r.id, f.transform(r.value), r.number)
        );
        Serializer<TestRecord> serializer = new CryptoFieldsSerializer<>(serde.serializer(), recordTransformer, dummySecretProvider);
        TestRecord result = serde.deserializer().deserialize(topic, serializer.serialize(topic, record));
        assertNotEquals(record, result);
    }
}
