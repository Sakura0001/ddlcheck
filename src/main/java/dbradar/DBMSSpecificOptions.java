package dbradar;

import java.util.List;

public interface DBMSSpecificOptions {

    List<? extends OracleFactory> getTestOracleFactory();

}
