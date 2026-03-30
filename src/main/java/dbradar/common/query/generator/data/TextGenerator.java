package dbradar.common.query.generator.data;

import dbradar.GlobalState;
import dbradar.Randomly;

public class TextGenerator implements Generator {

    private static final String ALPHANUMERIC_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    private int strLength;

    public TextGenerator(int length) {
        strLength = length;
    }

    @Override
    public String generate(GlobalState state) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < strLength; i++) {
            sb.append(ALPHANUMERIC_ALPHABET.charAt(Randomly.getNotCachedInteger(0, ALPHANUMERIC_ALPHABET.length())));
        }
        return String.format("'%s'", sb);
    }
}
