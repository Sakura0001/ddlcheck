package dbradar.common.query.generator.data;

import dbradar.GlobalState;
import dbradar.Randomly;

public class DecimalGenerator implements Generator {

    private int a = -1;
    private int b = -1;

    public DecimalGenerator(int a, int b) {
        this.a = a;
        this.b = b;
    }

    public DecimalGenerator() {

    }

    @Override
    public String generate(GlobalState state) {
        StringBuilder intPart = new StringBuilder();
        StringBuilder decimalPart = new StringBuilder();
        if (a == -1 && b == -1) {
            intPart.append(Randomly.getNotCachedInteger(0, 100));
            decimalPart.append(String.format("%04d", Randomly.getNotCachedInteger(0, 2000)));
        } else {
            for (int i = 0; i < (a - b); i++) {
                if (i == 0) {
                    intPart.append(Randomly.getNotCachedInteger(1, 10));
                } else {
                    intPart.append(Randomly.getNotCachedInteger(0, 10));
                }
            }
            for (int i = 0; i < b; i++) {
                decimalPart.append(Randomly.getNotCachedInteger(0, 10));
            }
        }
        return intPart + "." + decimalPart;
    }
}
