package dbradar.common.query.generator.data;

import java.util.List;

import dbradar.GlobalState;
import dbradar.Randomly;

public class ComposeGenerator implements Generator {

    private List<Generator> generators;

    public ComposeGenerator(List<Generator> generators) {
        this.generators = generators;
    }

    @Override
    public String generate(GlobalState state) {
        return generators.get(Randomly.getNotCachedInteger(0, generators.size())).generate(state);
    }
}
