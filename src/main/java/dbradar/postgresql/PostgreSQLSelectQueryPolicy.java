package dbradar.postgresql;

import dbradar.common.query.generator.QueryGenerationException;

import java.util.List;
import java.util.Locale;

public final class PostgreSQLSelectQueryPolicy {

    private static final int MAX_QUERY_LENGTH = 2500;
    private static final int MAX_SELECT_COUNT = 8;
    private static final int MAX_JOIN_COUNT = 4;
    private static final int MAX_COMPOUND_COUNT = 2;
    private static final int MAX_LIMIT_VALUE = 100;

    private static final List<String> BANNED_TOKENS = List.of(
            "PG_SLEEP",
            "PG_SLEEP_FOR",
            "PG_SLEEP_UNTIL",
            "SETSEED",
            "RANDOM(",
            "PG_ADVISORY",
            "LOCK TABLE");

    private PostgreSQLSelectQueryPolicy() {
    }

    public static void validateGeneratedSelect(String query) {
        String normalized = query.toUpperCase(Locale.ROOT);
        for (String bannedToken : BANNED_TOKENS) {
            if (normalized.contains(bannedToken)) {
                throw new QueryGenerationException("Generated SELECT contains banned token: " + bannedToken);
            }
        }
        if (query.length() > MAX_QUERY_LENGTH) {
            throw new QueryGenerationException("Generated SELECT is too long: " + query.length());
        }
        if (countKeyword(normalized, "SELECT") > MAX_SELECT_COUNT) {
            throw new QueryGenerationException("Generated SELECT has too many nested SELECT clauses");
        }
        if (countKeyword(normalized, "JOIN") > MAX_JOIN_COUNT) {
            throw new QueryGenerationException("Generated SELECT has too many JOIN clauses");
        }
        int compoundCount = countKeyword(normalized, "UNION")
                + countKeyword(normalized, "INTERSECT")
                + countKeyword(normalized, "EXCEPT");
        if (compoundCount > MAX_COMPOUND_COUNT) {
            throw new QueryGenerationException("Generated SELECT has too many compound operators");
        }
        int limitValue = extractLimitValue(normalized);
        if (limitValue > MAX_LIMIT_VALUE) {
            throw new QueryGenerationException("Generated SELECT LIMIT is too large: " + limitValue);
        }
    }

    private static int countKeyword(String query, String keyword) {
        int count = 0;
        int fromIndex = 0;
        while (fromIndex >= 0) {
            int found = query.indexOf(keyword, fromIndex);
            if (found < 0) {
                return count;
            }
            boolean leftBoundary = found == 0 || !Character.isLetterOrDigit(query.charAt(found - 1));
            int rightIndex = found + keyword.length();
            boolean rightBoundary = rightIndex == query.length() || !Character.isLetterOrDigit(query.charAt(rightIndex));
            if (leftBoundary && rightBoundary) {
                count++;
            }
            fromIndex = found + keyword.length();
        }
        return count;
    }

    private static int extractLimitValue(String query) {
        int limitIndex = query.indexOf("LIMIT ");
        if (limitIndex < 0) {
            return -1;
        }
        int numberStart = limitIndex + "LIMIT ".length();
        int numberEnd = numberStart;
        while (numberEnd < query.length() && Character.isDigit(query.charAt(numberEnd))) {
            numberEnd++;
        }
        if (numberEnd == numberStart) {
            return -1;
        }
        try {
            return Integer.parseInt(query.substring(numberStart, numberEnd));
        } catch (NumberFormatException ignored) {
            throw new QueryGenerationException("Generated SELECT LIMIT is too large to parse");
        }
    }
}
