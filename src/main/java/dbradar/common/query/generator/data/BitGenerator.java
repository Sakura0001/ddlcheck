package dbradar.common.query.generator.data;

import java.util.Arrays;

import dbradar.GlobalState;
import dbradar.Randomly;

public class BitGenerator implements Generator {

    private final int maxBitLength;
    private final boolean fixedLength;

    public BitGenerator() {
        this(65, false);
    }

    public BitGenerator(int maxBitLength) {
        this(maxBitLength, false);
    }

    public BitGenerator(int maxBitLength, boolean fixedLength) {
        this.maxBitLength = Math.max(1, maxBitLength);
        this.fixedLength = fixedLength;
    }

    @Override
    public String generate(GlobalState state) {
        int bitLength = fixedLength ? maxBitLength : Randomly.getNotCachedInteger(1, maxBitLength + 1);
        StringBuilder sb = new StringBuilder();
        sb.append("b'");
        for (int i = 0; i < bitLength; i++) {
            sb.append(Randomly.fromList(Arrays.asList("0", "1")));
        }
        sb.append("'");
        return String.format("%s", sb);
    }
}
