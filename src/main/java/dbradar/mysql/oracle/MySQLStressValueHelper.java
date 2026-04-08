package dbradar.mysql.oracle;

import dbradar.GlobalState;
import dbradar.Randomly;
import dbradar.common.query.generator.data.Generator;
import dbradar.common.query.generator.data.GeneratorRegister;
import dbradar.common.schema.AbstractTableColumn;
import dbradar.mysql.schema.MySQLSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class MySQLStressValueHelper {

    private MySQLStressValueHelper() {
    }

    public static String generateStressSafeValue(AbstractTableColumn<?, ?> column, GlobalState globalState) {
        if (column.isGenerated()) {
            throw new IllegalArgumentException("generated columns are not writable");
        }
        if (!(column instanceof MySQLSchema.MySQLColumn)) {
            Generator generator = GeneratorRegister.getGenerator(column, globalState);
            return generator.generate(globalState);
        }
        MySQLSchema.MySQLColumn mySQLColumn = (MySQLSchema.MySQLColumn) column;
        String dataType = Objects.toString(mySQLColumn.getDataType(), "").toLowerCase();
        switch (dataType) {
            case "tinyint":
            case "int1":
            case "bool":
            case "boolean":
                return Randomly.fromOptions("0", "1");
            case "smallint":
            case "int2":
                return Randomly.fromOptions("0", "1", "127");
            case "mediumint":
            case "int3":
            case "int":
            case "integer":
            case "int4":
            case "bigint":
            case "int8":
                return Randomly.fromOptions("0", "1", "42");
            case "year":
                return Randomly.fromOptions("1970", "2000", "2024");
            case "bit":
                return Randomly.fromOptions("b'0'", "b'1'");
            case "float":
            case "double":
                return Randomly.fromOptions("0", "1", "3.14");
            case "decimal":
            case "numeric":
            case "dec":
            case "fixed":
                return generateSafeFixedPointLiteral(mySQLColumn);
            case "date":
                return Randomly.fromOptions("'2000-01-01'", "'2024-12-31'");
            case "time":
                return Randomly.fromOptions("'00:00:01'", "'12:00:00'", "'23:59:59'");
            case "datetime":
            case "timestamp":
                return Randomly.fromOptions("'2000-01-01 00:00:01'", "'2024-12-31 23:59:59'");
            case "binary":
            case "varbinary":
                return generateSafeBinaryLiteral(mySQLColumn);
            case "blob":
            case "tinyblob":
            case "mediumblob":
            case "longblob":
                return Randomly.fromOptions("X'00'", "X'01'", "X'4142'");
            case "char":
            case "varchar":
                return generateSafeTextLiteral(mySQLColumn);
            case "text":
            case "tinytext":
            case "mediumtext":
            case "longtext":
                return generateSafeTextLiteral(null);
            case "enum":
                return generateQuotedLiteral("a", "b");
            case "set":
                return generateQuotedLiteral("a", "b", "a,b");
            default:
                return generateQuotedLiteral("a", "b", "test", "");
        }
    }

    private static String generateSafeFixedPointLiteral(MySQLSchema.MySQLColumn column) {
        int precision = Math.max(1, column.getNumericPrecision());
        int scale = Math.max(0, column.getNumericScale());
        int integerDigits = Math.max(0, precision - scale);

        List<String> candidates = new ArrayList<>();
        candidates.add("0");
        if (integerDigits > 0) {
            candidates.add("1");
        }
        if (scale > 0) {
            String fractional = "0." + "1" + "0".repeat(Math.max(0, scale - 1));
            candidates.add(fractional);
            if (integerDigits > 0) {
                candidates.add("1." + "0".repeat(Math.max(0, scale - 1)) + "1");
            }
        }
        return Randomly.fromList(candidates);
    }

    private static String generateSafeTextLiteral(MySQLSchema.MySQLColumn column) {
        if (column == null || column.getCharacterMaximumLength() == null || column.getCharacterMaximumLength() <= 0) {
            return generateQuotedLiteral("a", "b", "test", "");
        }

        long maxLength = column.getCharacterMaximumLength();
        List<String> values = new ArrayList<>();
        for (String candidate : new String[]{"", "a", "b", "ab", "test"}) {
            if (candidate.length() <= maxLength) {
                values.add(candidate);
            }
        }
        if (values.isEmpty()) {
            values.add("");
        }
        return generateQuotedLiteral(values.toArray(String[]::new));
    }

    private static String generateSafeBinaryLiteral(MySQLSchema.MySQLColumn column) {
        if (column.getCharacterMaximumLength() == null || column.getCharacterMaximumLength() <= 0) {
            return Randomly.fromOptions("X'00'", "X'01'", "X'4142'");
        }

        long maxLength = column.getCharacterMaximumLength();
        List<String> values = new ArrayList<>();
        for (String candidate : new String[]{"00", "01", "4142"}) {
            if (candidate.length() / 2 <= maxLength) {
                values.add("X'" + candidate + "'");
            }
        }
        if (values.isEmpty()) {
            values.add("X'00'");
        }
        return Randomly.fromList(values);
    }

    private static String generateQuotedLiteral(String... values) {
        List<String> quotedValues = new ArrayList<>();
        for (String value : values) {
            quotedValues.add("'" + value + "'");
        }
        return Randomly.fromList(quotedValues);
    }
}
