package dbradar.common.query.generator.data;

import dbradar.GlobalState;

public class InetGenerator implements Generator {
    @Override
    public String generate(GlobalState state) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            if (i != 0) {
                sb.append('.');
            }
            sb.append(state.getRandomly().getInteger() & 255);
        }
        return "'" + sb + "'";
    }
}
