package dbradar.postgresql;

import dbradar.IgnoreMeException;
import dbradar.Randomly;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import dbradar.postgresql.PostgreSQLSchema.PostgreSQLTable;
import dbradar.postgresql.PostgreSQLSchema.PostgreSQLTable.PartitionStrategy;

public final class PostgreSQLPartitionSupport {

    private static final Pattern PARTITION_KEY_PATTERN = Pattern.compile("^(RANGE|LIST|HASH) \\((.+)\\)$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern RANGE_BOUND_PATTERN = Pattern.compile("^FOR VALUES FROM \\((.+)\\) TO \\((.+)\\)$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern LIST_BOUND_PATTERN = Pattern.compile("^FOR VALUES IN \\((.+)\\)$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern HASH_BOUND_PATTERN = Pattern.compile(
            "^FOR VALUES WITH \\(modulus (\\d+), remainder (\\d+)\\)$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern GENERATED_PARTITION_KEY_COLUMN_PATTERN = Pattern.compile("\\bpartition_key\\d+\\b",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern SIMPLE_IDENTIFIER_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private PostgreSQLPartitionSupport() {
    }

    public static List<String> parsePartitionKeyColumns(String partitionKeyDefinition) {
        if (partitionKeyDefinition == null) {
            return Collections.emptyList();
        }
        Matcher matcher = PARTITION_KEY_PATTERN.matcher(partitionKeyDefinition.trim());
        if (!matcher.matches()) {
            return Collections.emptyList();
        }
        String keyDefinition = matcher.group(2);
        if (!isSimpleColumnList(keyDefinition)) {
            return extractGeneratedPartitionKeyColumns(keyDefinition);
        }
        return splitTopLevelAndTrim(keyDefinition);
    }

    public static int parsePartitionKeyArity(String partitionKeyDefinition) {
        if (partitionKeyDefinition == null) {
            return 0;
        }
        Matcher matcher = PARTITION_KEY_PATTERN.matcher(partitionKeyDefinition.trim());
        if (!matcher.matches()) {
            return 0;
        }
        return splitTopLevelAndTrim(matcher.group(2)).size();
    }

    public static boolean isExpressionPartitionKey(String partitionKeyDefinition) {
        if (partitionKeyDefinition == null) {
            return false;
        }
        Matcher matcher = PARTITION_KEY_PATTERN.matcher(partitionKeyDefinition.trim());
        return matcher.matches() && !isSimpleColumnList(matcher.group(2));
    }

    public static boolean supportsDefaultPartition(PostgreSQLTable parent) {
        return parent.getPartitionStrategy() == PartitionStrategy.RANGE
                || parent.getPartitionStrategy() == PartitionStrategy.LIST;
    }

    public static boolean canCreateAdditionalPartition(PostgreSQLSchema schema, PostgreSQLTable parent) {
        if (!parent.isPartitionedTable()) {
            return false;
        }
        return switch (parent.getPartitionStrategy()) {
            case RANGE, LIST -> !schema.hasDefaultPartition(parent);
            case HASH -> !hasCompleteHashCoverage(schema, parent);
            case NONE -> false;
        };
    }

    public static boolean hasUsableInsertRoute(PostgreSQLSchema schema, PostgreSQLTable parent) {
        if (!parent.isPartitionedTable()) {
            return false;
        }
        return switch (parent.getPartitionStrategy()) {
            case RANGE -> schema.hasDefaultPartition(parent) || !getExplicitRangePartitions(schema, parent).isEmpty();
            case LIST -> schema.hasDefaultPartition(parent) || !getExplicitListPartitions(schema, parent).isEmpty();
            case HASH -> hasCompleteHashCoverage(schema, parent);
            case NONE -> false;
        };
    }

    public static String createPartitionBound(PostgreSQLSchema schema, PostgreSQLTable parent) {
        return switch (parent.getPartitionStrategy()) {
            case RANGE -> createRangePartitionBound(schema, parent);
            case LIST -> createListPartitionBound(schema, parent);
            case HASH -> createHashPartitionBound(schema, parent);
            case NONE -> throw new IgnoreMeException("Selected table is not partitioned.");
        };
    }

    public static Map<String, String> generateInsertValues(PostgreSQLSchema schema, PostgreSQLTable parent) {
        if (!hasUsableInsertRoute(schema, parent)) {
            throw new IgnoreMeException("Partitioned table has no usable insert route.");
        }
        return switch (parent.getPartitionStrategy()) {
            case RANGE -> generateRangeInsertValues(schema, parent);
            case LIST -> generateListInsertValues(schema, parent);
            case HASH -> generateHashInsertValues(parent);
            case NONE -> Collections.emptyMap();
        };
    }

    public static boolean hasCompleteHashCoverage(PostgreSQLSchema schema, PostgreSQLTable parent) {
        HashCoverage coverage = getHashCoverage(schema, parent);
        if (coverage == null) {
            return false;
        }
        for (int remainder = 0; remainder < coverage.modulus; remainder++) {
            if (!coverage.remainders.contains(remainder)) {
                return false;
            }
        }
        return true;
    }

    private static String createRangePartitionBound(PostgreSQLSchema schema, PostgreSQLTable parent) {
        List<RangePartitionBound> explicitPartitions = getExplicitRangePartitions(schema, parent);
        if (!explicitPartitions.isEmpty() && Randomly.getBooleanWithRatherLowProbability()) {
            return "DEFAULT";
        }

        int keyCount = parent.getPartitionKeyArity();
        if (keyCount <= 0) {
            throw new IgnoreMeException("Range partitioned table does not expose partition keys.");
        }
        int lowerBoundBase = explicitPartitions.stream()
                .mapToInt(bound -> bound.toValues().get(0))
                .max()
                .orElse(0);
        int upperBoundBase = explicitPartitions.isEmpty() ? 100 : lowerBoundBase + 100;
        if (!explicitPartitions.isEmpty()) {
            lowerBoundBase = explicitPartitions.stream()
                    .mapToInt(bound -> bound.toValues().get(0))
                    .max()
                    .orElse(0);
            upperBoundBase = lowerBoundBase + 100;
        }

        List<Integer> fromValues = repeatValue(keyCount, lowerBoundBase);
        List<Integer> toValues = repeatValue(keyCount, upperBoundBase);
        return String.format("FOR VALUES FROM (%s) TO (%s)",
                joinIntegers(fromValues), joinIntegers(toValues));
    }

    private static String createListPartitionBound(PostgreSQLSchema schema, PostgreSQLTable parent) {
        List<ListPartitionBound> explicitPartitions = getExplicitListPartitions(schema, parent);
        if (!explicitPartitions.isEmpty() && Randomly.getBooleanWithRatherLowProbability()) {
            return "DEFAULT";
        }

        Set<Integer> usedValues = new LinkedHashSet<>();
        for (ListPartitionBound bound : explicitPartitions) {
            usedValues.addAll(bound.values());
        }
        int nextValue = usedValues.stream().mapToInt(Integer::intValue).max().orElse(0) + 1;
        int valueCount = Randomly.getNotCachedInteger(1, 4);
        List<Integer> values = new ArrayList<>();
        for (int i = 0; i < valueCount; i++) {
            values.add(nextValue + i);
        }
        return String.format("FOR VALUES IN (%s)", joinIntegers(values));
    }

    private static String createHashPartitionBound(PostgreSQLSchema schema, PostgreSQLTable parent) {
        HashCoverage coverage = getHashCoverage(schema, parent);
        int modulus = coverage == null ? Randomly.getNotCachedInteger(2, 5) : coverage.modulus;
        Set<Integer> usedRemainders = coverage == null ? Collections.emptySet() : coverage.remainders;
        List<Integer> missingRemainders = new ArrayList<>();
        for (int remainder = 0; remainder < modulus; remainder++) {
            if (!usedRemainders.contains(remainder)) {
                missingRemainders.add(remainder);
            }
        }
        if (missingRemainders.isEmpty()) {
            throw new IgnoreMeException("Hash partitioned table already has complete remainder coverage.");
        }
        int remainder = Randomly.fromList(missingRemainders);
        return String.format("FOR VALUES WITH (MODULUS %d, REMAINDER %d)", modulus, remainder);
    }

    private static Map<String, String> generateRangeInsertValues(PostgreSQLSchema schema, PostgreSQLTable parent) {
        List<String> keyColumns = parent.getPartitionKeyColumns();
        int keyArity = parent.getPartitionKeyArity();
        if (keyColumns.isEmpty() || keyArity <= 0) {
            throw new IgnoreMeException("Range partitioned table does not expose partition keys.");
        }

        List<RangePartitionBound> explicitPartitions = getExplicitRangePartitions(schema, parent);
        List<Integer> values;
        boolean chooseDefault = schema.hasDefaultPartition(parent)
                && (!explicitPartitions.isEmpty() && Randomly.getBooleanWithRatherLowProbability());
        if (chooseDefault || explicitPartitions.isEmpty()) {
            int maxUpper = explicitPartitions.stream()
                    .mapToInt(bound -> bound.toValues().get(0))
                    .max()
                    .orElse(0);
            values = repeatValue(keyArity, maxUpper + 25);
        } else {
            RangePartitionBound bound = Randomly.fromList(explicitPartitions);
            values = new ArrayList<>();
            for (int i = 0; i < keyArity; i++) {
                int lower = bound.fromValues().get(i);
                int upper = bound.toValues().get(i);
                int candidate = lower + Math.max((upper - lower) / 2, 0);
                if (candidate >= upper) {
                    candidate = lower;
                }
                values.add(candidate);
            }
        }
        if (parent.isExpressionPartitionKey()) {
            return toExpressionRangeValueMap(keyColumns, values.get(0));
        }
        return toValueMap(keyColumns, values);
    }

    private static Map<String, String> generateListInsertValues(PostgreSQLSchema schema, PostgreSQLTable parent) {
        List<String> keyColumns = parent.getPartitionKeyColumns();
        if (keyColumns.size() != 1) {
            throw new IgnoreMeException("LIST partitioning only supports a single generated key.");
        }

        List<ListPartitionBound> explicitPartitions = getExplicitListPartitions(schema, parent);
        int value;
        boolean chooseDefault = schema.hasDefaultPartition(parent)
                && (!explicitPartitions.isEmpty() && Randomly.getBooleanWithRatherLowProbability());
        if (chooseDefault || explicitPartitions.isEmpty()) {
            int maxValue = explicitPartitions.stream()
                    .flatMap(bound -> bound.values().stream())
                    .mapToInt(Integer::intValue)
                    .max()
                    .orElse(0);
            value = maxValue + 1;
        } else {
            ListPartitionBound bound = Randomly.fromList(explicitPartitions);
            value = Randomly.fromList(bound.values());
        }
        return Map.of(keyColumns.get(0), Integer.toString(value));
    }

    private static Map<String, String> generateHashInsertValues(PostgreSQLTable parent) {
        if (parent.getPartitionKeyColumns().isEmpty()) {
            throw new IgnoreMeException("Hash partitioned table does not expose partition keys.");
        }
        Map<String, String> values = new HashMap<>();
        for (String columnName : parent.getPartitionKeyColumns()) {
            values.put(columnName, Integer.toString(Randomly.getNotCachedInteger(0, 1000)));
        }
        return values;
    }

    private static List<RangePartitionBound> getExplicitRangePartitions(PostgreSQLSchema schema, PostgreSQLTable parent) {
        List<RangePartitionBound> bounds = new ArrayList<>();
        for (PostgreSQLTable partition : schema.getPartitions(parent)) {
            if (partition.isDefaultPartition()) {
                continue;
            }
            RangePartitionBound bound = parseRangeBound(partition.getPartitionBound());
            if (bound != null) {
                bounds.add(bound);
            }
        }
        return bounds;
    }

    private static List<ListPartitionBound> getExplicitListPartitions(PostgreSQLSchema schema, PostgreSQLTable parent) {
        List<ListPartitionBound> bounds = new ArrayList<>();
        for (PostgreSQLTable partition : schema.getPartitions(parent)) {
            if (partition.isDefaultPartition()) {
                continue;
            }
            ListPartitionBound bound = parseListBound(partition.getPartitionBound());
            if (bound != null) {
                bounds.add(bound);
            }
        }
        return bounds;
    }

    private static HashCoverage getHashCoverage(PostgreSQLSchema schema, PostgreSQLTable parent) {
        List<HashPartitionBound> bounds = new ArrayList<>();
        for (PostgreSQLTable partition : schema.getPartitions(parent)) {
            if (partition.isDefaultPartition()) {
                continue;
            }
            HashPartitionBound bound = parseHashBound(partition.getPartitionBound());
            if (bound == null) {
                return null;
            }
            bounds.add(bound);
        }
        if (bounds.isEmpty()) {
            return null;
        }
        int coverageModulus = 1;
        for (HashPartitionBound bound : bounds) {
            coverageModulus = lcm(coverageModulus, bound.modulus());
        }
        Set<Integer> remainders = new LinkedHashSet<>();
        for (HashPartitionBound bound : bounds) {
            for (int remainder = 0; remainder < coverageModulus; remainder++) {
                if (remainder % bound.modulus() == bound.remainder()) {
                    remainders.add(remainder);
                }
            }
        }
        return new HashCoverage(coverageModulus, remainders);
    }

    private static RangePartitionBound parseRangeBound(String partitionBound) {
        if (partitionBound == null) {
            return null;
        }
        Matcher matcher = RANGE_BOUND_PATTERN.matcher(partitionBound.trim());
        if (!matcher.matches()) {
            return null;
        }
        List<Integer> fromValues = parseIntegerTuple(matcher.group(1));
        List<Integer> toValues = parseIntegerTuple(matcher.group(2));
        if (fromValues.isEmpty() || fromValues.size() != toValues.size()) {
            return null;
        }
        return new RangePartitionBound(fromValues, toValues);
    }

    private static ListPartitionBound parseListBound(String partitionBound) {
        if (partitionBound == null) {
            return null;
        }
        Matcher matcher = LIST_BOUND_PATTERN.matcher(partitionBound.trim());
        if (!matcher.matches()) {
            return null;
        }
        List<Integer> values = parseIntegerTuple(matcher.group(1));
        if (values.isEmpty()) {
            return null;
        }
        return new ListPartitionBound(values);
    }

    private static HashPartitionBound parseHashBound(String partitionBound) {
        if (partitionBound == null) {
            return null;
        }
        Matcher matcher = HASH_BOUND_PATTERN.matcher(partitionBound.trim());
        if (!matcher.matches()) {
            return null;
        }
        return new HashPartitionBound(Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)));
    }

    private static List<Integer> parseIntegerTuple(String values) {
        List<Integer> parsed = new ArrayList<>();
        for (String rawValue : splitAndTrim(values)) {
            if (!rawValue.matches("-?\\d+")) {
                return Collections.emptyList();
            }
            parsed.add(Integer.parseInt(rawValue));
        }
        return parsed;
    }

    private static List<String> splitAndTrim(String rawValues) {
        if (rawValues == null || rawValues.isBlank()) {
            return Collections.emptyList();
        }
        String[] parts = rawValues.split(",");
        List<String> values = new ArrayList<>(parts.length);
        for (String part : parts) {
            values.add(part.trim());
        }
        return values;
    }

    private static List<String> splitTopLevelAndTrim(String rawValues) {
        if (rawValues == null || rawValues.isBlank()) {
            return Collections.emptyList();
        }
        List<String> values = new ArrayList<>();
        int start = 0;
        int depth = 0;
        for (int i = 0; i < rawValues.length(); i++) {
            char ch = rawValues.charAt(i);
            if (ch == '(') {
                depth++;
            } else if (ch == ')') {
                depth = Math.max(0, depth - 1);
            } else if (ch == ',' && depth == 0) {
                values.add(rawValues.substring(start, i).trim());
                start = i + 1;
            }
        }
        values.add(rawValues.substring(start).trim());
        values.removeIf(String::isEmpty);
        return values;
    }

    private static boolean isSimpleColumnList(String keyDefinition) {
        List<String> keys = splitTopLevelAndTrim(keyDefinition);
        if (keys.isEmpty()) {
            return false;
        }
        for (String key : keys) {
            if (!SIMPLE_IDENTIFIER_PATTERN.matcher(key).matches()) {
                return false;
            }
        }
        return true;
    }

    private static List<String> extractGeneratedPartitionKeyColumns(String keyDefinition) {
        Set<String> columns = new LinkedHashSet<>();
        Matcher matcher = GENERATED_PARTITION_KEY_COLUMN_PATTERN.matcher(keyDefinition);
        while (matcher.find()) {
            columns.add(matcher.group().toLowerCase());
        }
        return new ArrayList<>(columns);
    }

    private static String joinIntegers(List<Integer> values) {
        return values.stream().map(String::valueOf).collect(java.util.stream.Collectors.joining(", "));
    }

    private static List<Integer> repeatValue(int count, int value) {
        List<Integer> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(value);
        }
        return values;
    }

    private static Map<String, String> toValueMap(List<String> keyColumns, List<Integer> values) {
        Map<String, String> valueMap = new HashMap<>();
        for (int i = 0; i < keyColumns.size(); i++) {
            valueMap.put(keyColumns.get(i), Integer.toString(values.get(i)));
        }
        return valueMap;
    }

    private static Map<String, String> toExpressionRangeValueMap(List<String> keyColumns, int targetValue) {
        if (!keyColumns.contains("partition_key1") || !keyColumns.contains("partition_key2")) {
            throw new IgnoreMeException("Unsupported generated expression partition key.");
        }
        int left = Math.floorDiv(targetValue, 2);
        int right = targetValue - left;
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("partition_key1", Integer.toString(left));
        valueMap.put("partition_key2", Integer.toString(right));
        return valueMap;
    }

    private static int lcm(int left, int right) {
        return left / gcd(left, right) * right;
    }

    private static int gcd(int left, int right) {
        int a = Math.abs(left);
        int b = Math.abs(right);
        while (b != 0) {
            int tmp = a % b;
            a = b;
            b = tmp;
        }
        return a == 0 ? 1 : a;
    }

    private record RangePartitionBound(List<Integer> fromValues, List<Integer> toValues) {
    }

    private record ListPartitionBound(List<Integer> values) {
    }

    private record HashPartitionBound(int modulus, int remainder) {
    }

    private record HashCoverage(int modulus, Set<Integer> remainders) {
    }
}
