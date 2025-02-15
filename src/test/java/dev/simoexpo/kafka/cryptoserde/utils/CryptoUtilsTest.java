package dev.simoexpo.kafka.cryptoserde.utils;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class CryptoUtilsTest {

    @Test
    public void shouldEncryptAndDecrypt() throws GeneralSecurityException {
        String data1 = "Secret info";
        String secret = "A perfect and secure AES secret!";
        byte[] encrypted = CryptoUtils.encrypt(data1.getBytes(StandardCharsets.UTF_8), secret);
        byte[] decrypted = CryptoUtils.decrypt(encrypted, secret);
        assertFalse(Arrays.equals(encrypted, data1.getBytes(StandardCharsets.UTF_8)));
        assertArrayEquals(data1.getBytes(StandardCharsets.UTF_8), decrypted);
    }

    @Test
    public void shouldEncryptTheSameDataInDifferentWay() throws GeneralSecurityException {
        String data1 = "Secret info";
        String secret = "A perfect and secure AES secret!";
        byte[] result1 = CryptoUtils.encrypt(data1.getBytes(StandardCharsets.UTF_8), secret);
        byte[] result2 = CryptoUtils.encrypt(data1.getBytes(StandardCharsets.UTF_8), secret);
        assertFalse(Arrays.equals(result1, result2));

    }
}
