package dev.simoexpo.kafka.cryptoserde;

import dev.simoexpo.kafka.cryptoserde.secrets.SecretProvider;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class CryptoRecordSerdeTest {

    record TestRecord(String id, String value, int number) {}

    String topic = "test-topic";
    JsonSerde<TestRecord> serde = new JsonSerde<>(TestRecord.class);
    SecretProvider dummySecretProvider = key -> Optional.of(key.repeat(32 / key.length() + 1).substring(0, 32));

    @Test
    public void shouldSerializeAndDeserialize() {
        TestRecord record = new TestRecord("1", "value",1);
        Serializer<TestRecord> serializer = new CryptoRecordSerializer<>(
                serde.serializer(),
                s -> s.id,
                dummySecretProvider);
        Deserializer<TestRecord> deserializer = new CryptoRecordDeserializer<>(
                serde.deserializer(),
                dummySecretProvider
        );
        TestRecord result = deserializer.deserialize(topic, serializer.serialize(topic, record));
        assertEquals(record, result);
    }

    @Test
    public void shouldNotSerializeWhenSecretDoesNotExist() {
        TestRecord record = new TestRecord("1", "value",1);
        Serializer<TestRecord> serializer = new CryptoRecordSerializer<>(
                serde.serializer(),
                s -> s.id,
                k -> Optional.empty());
        assertNull(serializer.serialize(topic, record));
    }

    @Test
    public void shouldNotDeserializeWhenSecretDoesNotExist() {
        TestRecord record = new TestRecord("1", "value",1);
        Serializer<TestRecord> serializer = new CryptoRecordSerializer<>(
                serde.serializer(),
                s -> s.id,
                dummySecretProvider);
        Deserializer<TestRecord> deserializer = new CryptoRecordDeserializer<>(
                serde.deserializer(),
                k -> Optional.empty()
        );
        TestRecord result = deserializer.deserialize(topic, serializer.serialize(topic, record));
        assertNull(result);
    }

    @Test
    public void shouldNotDeserializeWithStandardDeserializer() {
        TestRecord record = new TestRecord("1", "value",1);
        Serializer<TestRecord> serializer = new CryptoRecordSerializer<>(
                serde.serializer(),
                s -> s.id,
                dummySecretProvider);
        byte[] serialized = serializer.serialize(topic, record);
        assertThrows(SerializationException.class, () -> serde.deserializer().deserialize(topic, serialized));
    }
}
