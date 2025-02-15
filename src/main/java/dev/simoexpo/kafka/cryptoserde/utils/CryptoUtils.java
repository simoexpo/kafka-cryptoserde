package dev.simoexpo.kafka.cryptoserde.utils;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

public class CryptoUtils {

    private static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String ALGORITHM = "AES";
    private static final String SEPARATOR = "#";

    public static byte[] encrypt(byte[] bytes, String secret) throws GeneralSecurityException {
        byte[] iv = new byte[16];
        SecureRandom random = new SecureRandom();
        random.nextBytes(iv);
        Key secretKey = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM);
        Cipher cipher = getCipher();
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, getGCMParamSpec(iv));
        byte[] encryptedData = cipher.doFinal(bytes);
        Base64.Encoder encoder = Base64.getEncoder();
        byte[] encrypt64 = encoder.encode(encryptedData);
        byte[] iv64 = encoder.encode(iv);
        return (new String(encrypt64) + SEPARATOR + new String(iv64)).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] decrypt(byte[] bytes, String secret) throws GeneralSecurityException {
        String[] split = new String(bytes, StandardCharsets.UTF_8).split(SEPARATOR);
        Base64.Decoder decoder = Base64.getDecoder();
        byte[] cypherText = decoder.decode(split[0]);
        byte[] iv = decoder.decode(split[1]);
        Key secretKey = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM);
        Cipher cipher = getCipher();
        cipher.init(Cipher.DECRYPT_MODE, secretKey, getGCMParamSpec(iv));
        return cipher.doFinal(cypherText);
    }

    private static GCMParameterSpec getGCMParamSpec(byte[] iv) {
        return new GCMParameterSpec(128, iv);
    }

    private static Cipher getCipher() throws NoSuchPaddingException, NoSuchAlgorithmException {
        return Cipher.getInstance(CIPHER_TRANSFORMATION);
    }

}
