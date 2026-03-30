package dbradar.common.query.generator.data;

import dbradar.GlobalState;

public class ConstGenerator implements Generator {
    /* as the default value for Generator */
    private String constant;

    public ConstGenerator(String constant) {
        this.constant = constant;
    }

    @Override
    public String generate(GlobalState state) {
        return constant;
    }
}
