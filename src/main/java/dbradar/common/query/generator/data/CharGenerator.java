package dbradar.common.query.generator.data;

import dbradar.GlobalState;
import dbradar.Randomly;

public class CharGenerator implements Generator {

    private static final String ALPHANUMERIC_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    @Override
    public String generate(GlobalState state) {
        char c = ALPHANUMERIC_ALPHABET.charAt(Randomly.getNotCachedInteger(0, ALPHANUMERIC_ALPHABET.length()));
        return String.format("'%c'", c);
    }
}
