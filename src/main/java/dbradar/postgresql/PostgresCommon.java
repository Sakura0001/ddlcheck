package dbradar.postgresql;

import dbradar.common.query.ExpectedErrors;

import java.util.regex.Pattern;

public final class PostgresCommon {

    private PostgresCommon() {
    }

    public static void addCommonFetchErrors(ExpectedErrors errors) {
        errors.add("FULL JOIN is only supported with merge-joinable or hash-joinable join conditions");
        errors.add("but it cannot be referenced from this part of the query");
        errors.add("missing FROM-clause entry for table");

        errors.add("canceling statement due to statement timeout");

        errors.add("non-integer constant in GROUP BY");
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("GROUP BY position");
    }

    public static void addCommonExpressionErrors(ExpectedErrors errors) {
        errors.add("You might need to add explicit type casts");
        errors.add("invalid regular expression");
        errors.add("could not determine which collation to use");
        errors.add("invalid regular expression");
        errors.add("operator does not exist");
        errors.add("quantifier operand invalid");
        errors.add("collation mismatch");
        errors.add("collations are not supported");
        errors.add("operator is not unique");
        errors.add("is not a valid binary digit");
        errors.add("invalid hexadecimal digit");
        errors.add("invalid hexadecimal data: odd number of digits");
        errors.add("zero raised to a negative power is undefined");
        errors.addRegex(Pattern.compile("cannot convert infinity to \\w+"));
        errors.add("division by zero");
        errors.add("invalid input syntax for type money");
        errors.add("invalid input syntax for type");
        errors.add("cannot cast type");
        errors.add("value overflows numeric format");
        errors.add("LIKE pattern must not end with escape character");
        errors.add("is of type boolean but expression is of type text");
        errors.add("a negative number raised to a non-integer power yields a complex result");
        errors.add("could not determine polymorphic type because input has type unknown");
        errors.add("character number must be positive");
        addToCharFunctionErrors(errors);
        addBitStringOperationErrors(errors);
        addFunctionErrors(errors);
        addCommonRangeExpressionErrors(errors);
        addCommonRegexExpressionErrors(errors);
    }

    private static void addToCharFunctionErrors(ExpectedErrors errors) {
        errors.add("multiple decimal points");
        errors.add("and decimal point together");
        errors.add("multiple decimal points");
        errors.add("cannot use \"S\" twice");
        errors.add("must be ahead of \"PR\"");
        errors.add("cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together");
        errors.add("cannot use \"S\" and \"SG\" together");
        errors.add("cannot use \"S\" and \"MI\" together");
        errors.add("cannot use \"S\" and \"PL\" together");
        errors.add("cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together");
        errors.add("is not a number");
    }

    private static void addBitStringOperationErrors(ExpectedErrors errors) {
        errors.add("cannot XOR bit strings of different sizes");
        errors.add("cannot AND bit strings of different sizes");
        errors.add("cannot OR bit strings of different sizes");
        errors.add("must be type boolean, not type text");
    }

    private static void addFunctionErrors(ExpectedErrors errors) {
        errors.add("out of valid range"); // get_bit/get_byte
        errors.add("cannot take logarithm of a negative number");
        errors.add("cannot take logarithm of zero");
        errors.add("requested character too large for encoding"); // chr
        errors.add("null character not permitted"); // chr
        errors.add("requested character not valid for encoding"); // chr
        errors.add("requested length too large"); // repeat
        errors.add("invalid memory alloc request size"); // repeat

        /*
         * PostgreSQL support only a few conversion variants to ASCII: LATIN1, LATIN2, LATIN9 and WINDOWS1250. So, it is
         * better to skip this error at all.
         */
        errors.addRegex(Pattern.compile("encoding conversion from \\w+ to ASCII not supported"));

        /*
         * In accordance with PostgreSQL code, commit 0ab1a2e, conversions to or from SQL_ASCII is meaningless. So
         * disable errors on such an attempt.
         */
        errors.addRegex(Pattern.compile("encoding conversion from SQL_ASCII to \\w+ not supported"));
        errors.addRegex(Pattern.compile("encoding conversion from \\w+ to SQL_ASCII not supported"));

        errors.add("negative substring length not allowed"); // substr
        errors.add("invalid mask length"); // set_masklen
    }

    private static void addCommonRegexExpressionErrors(ExpectedErrors errors) {
        errors.add("is not a valid hexadecimal digit");
    }

    public static void addCommonRangeExpressionErrors(ExpectedErrors errors) {
        errors.add("range lower bound must be less than or equal to range upper bound");
        errors.add("result of range difference would not be contiguous");
        errors.add("out of range");
        errors.add("malformed range literal");
        errors.add("result of range union would not be contiguous");
    }
}
