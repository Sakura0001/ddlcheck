package dbradar.sqlite3.schema;

public enum SQLite3DataType {
    INTEGER, REAL, VARCHAR, BLOB, // SQL Type
    NONE;


    public boolean isNumeric() {
        switch (this) {
            case INTEGER:
            case REAL:
            case NONE:
                return true;
            default:
                return false;
        }
    }

    public boolean isTextOrBlob() {
        switch (this) {
            case VARCHAR:
            case BLOB:
                return true;
            default:
                return false;
        }
    }

    public static SQLite3DataType getSQLiteDateType(String columnTypeString) {
        String trimmedTypeString = columnTypeString.toLowerCase().replace(" generated always", "");
        switch (trimmedTypeString) {
            case "int":
            case "integer":
                return INTEGER;
            case "real":
                return REAL;
            case "text":
                return VARCHAR;
            case "blob":
                return BLOB;
            case "":
                return NONE; // used in virtual table
            default:
                throw new AssertionError("Unrecognized datatype " + trimmedTypeString);
        }
    }
}
