package com.couchbaseFight;

import java.security.SecureRandom;
import java.util.Random;

public class ValueGenerator {

    private final Random rand = new Random();
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";


    private String generateString(int length) {
        StringBuilder sb = new StringBuilder(length);
        SecureRandom random = new SecureRandom();

        for (int i = 0; i < length; i++) {
            int randomIndex = random.nextInt(CHARACTERS.length());
            char randomChar = CHARACTERS.charAt(randomIndex);
            sb.append(randomChar);
        }

        return sb.toString();
    }

    public String generateIPAddress() {
        return rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256);
    }

    public static boolean generateBoolean() {
        Random random = new Random();
        return random.nextBoolean();
    }

    public static int generateInteger(int min, int max) {
        Random random = new Random();
        return random.nextInt(max - min + 1) + min;
    }


    
}
