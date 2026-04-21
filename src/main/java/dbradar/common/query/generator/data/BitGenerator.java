package dbradar.common.query.generator.data;

import java.util.Arrays;

import dbradar.GlobalState;
import dbradar.Randomly;

public class BitGenerator implements Generator {

    @Override
    public String generate(GlobalState state) {
        int bitLength = Randomly.getNotCachedInteger(1,65);
        StringBuilder sb = new StringBuilder();
        sb.append("b'");
        for (int i = 0; i < bitLength; i++) {
            sb.append(Randomly.fromList(Arrays.asList("0", "1")));
        }
        sb.append("'");
        return String.format("%s", sb);
    }
}
