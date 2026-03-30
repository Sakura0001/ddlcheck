package dbradar.common.query.generator.data;

import dbradar.GlobalState;
import dbradar.Randomly;

public class BoolGenerator implements Generator {

    @Override
    public String generate(GlobalState state) {
        boolean randBool = Randomly.getBoolean();
        if (randBool) {
            return "true";
        } else {
            return "false";
        }
    }
}
