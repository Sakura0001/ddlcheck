package dbradar;

import com.beust.jcommander.JCommander;

public final class MainOptionsTask1Test {

    private MainOptionsTask1Test() {
    }

    public static void main(String[] args) {
        verifiesDefaultCounts();
        verifiesParsedCounts();
    }

    private static void verifiesDefaultCounts() {
        MainOptions options = new MainOptions();
        require(options.getDdlCount() > 0, "Expected a positive default ddl-count");
        require(options.getDmlCount() > 0, "Expected a positive default dml-count");
    }

    private static void verifiesParsedCounts() {
        MainOptions options = new MainOptions();
        JCommander.newBuilder().addObject(options).build()
                .parse("--ddl-count", "7", "--dml-count", "9");

        require(options.getDdlCount() == 7, "Expected parsed ddl-count to equal 7");
        require(options.getDmlCount() == 9, "Expected parsed dml-count to equal 9");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
