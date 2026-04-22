package dbradar.common.query.generator.data;

import dbradar.GlobalState;

import java.util.function.Function;

public class LambdaGenerator implements Generator {

    private final Function<GlobalState, String> generator;

    public LambdaGenerator(Function<GlobalState, String> generator) {
        this.generator = generator;
    }

    @Override
    public String generate(GlobalState state) {
        return generator.apply(state);
    }
}
