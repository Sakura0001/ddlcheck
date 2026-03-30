package dbradar.sqlite3.schema;

import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.SQLConnection;
import dbradar.common.schema.AbstractSchema;
import dbradar.sqlite3.SQLite3GlobalState;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SQLite3Schema extends AbstractSchema<SQLite3GlobalState, SQLite3Table, SQLite3Trigger> {

    private final List<SQLite3Trigger> triggers;

    public List<SQLite3Index> getIndexes() {
        List<SQLite3Index> indexes = new ArrayList<>();
        for (SQLite3Table table : getDatabaseTables()) {
            indexes.addAll(table.getIndexes());
        }

        return indexes;
    }

    public SQLite3Index getRandomIndex(Predicate<SQLite3Index> predicate) {
        List<SQLite3Index> relevantIndexes = getIndexes().stream().filter(predicate).collect(Collectors.toList());
        if (relevantIndexes.isEmpty()) {
            throw new IgnoreMeException();
        }

        return Randomly.fromList(relevantIndexes);
    }

    public SQLite3Index getRandomIndexOrBailout() {
        if (getIndexes().isEmpty()) {
            throw new IgnoreMeException();
        } else {
            return Randomly.fromList(getIndexes());
        }
    }

    public SQLite3Schema(List<SQLite3Table> databaseTables, List<SQLite3Trigger> triggers) {
        super(databaseTables, Collections.emptyList());
        this.triggers = triggers;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (SQLite3Table t : getDatabaseTables()) {
            sb.append(t);
            sb.append("\n");
        }
        return sb.toString();
    }

    public static SQLite3Schema fromConnection(SQLite3GlobalState globalState) throws SQLException {
        List<SQLite3Table> databaseTables = new ArrayList<>();
        List<SQLite3Trigger> triggers = new ArrayList<>();

        SQLConnection con = globalState.getConnection();

        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT name, type as category, sql FROM sqlite_master UNION "
                    + "SELECT name, 'temp_table' as category, sql FROM sqlite_temp_master WHERE type='table' UNION SELECT name, 'view' as category, sql FROM sqlite_temp_master WHERE type='view' GROUP BY name;")) {
                while (rs.next()) {
                    String tableName = rs.getString("name");
                    String tableType = rs.getString("category");
                    boolean isReadOnly;
                    if (databaseTables.stream().anyMatch(t -> t.getName().contentEquals(tableName))) {
                        continue;
                    }
                    String sqlString = rs.getString("sql") == null ? "" : rs.getString("sql").toLowerCase();
                    if (tableName.startsWith("sqlite_") || tableType.equals("index") || tableType.equals("trigger")
                            || tableName.endsWith("_idx") || tableName.endsWith("_docsize")
                            || tableName.endsWith("_content") || tableName.endsWith("_data")
                            || tableName.endsWith("_config") || tableName.endsWith("_segdir")
                            || tableName.endsWith("_stat") || tableName.endsWith("_segments")
                            || tableName.contains("_")) {
                        continue; // TODO
                    } else if (sqlString.contains("using dbstat")) {
                        isReadOnly = true;
                    } else if (sqlString.contains("content=''")) {
                        isReadOnly = true;
                    } else {
                        isReadOnly = false;
                    }
                    boolean withoutRowid = sqlString.contains("without rowid");
                    boolean isView = tableType.contentEquals("view");
                    boolean isVirtual = sqlString.contains("virtual");
                    boolean isDbStatsTable = sqlString.contains("using dbstat");
                    List<SQLite3Column> databaseColumns = SQLite3Column.getTableColumns(con, tableName, sqlString, isView,
                            isDbStatsTable);
                    SQLite3Table t = new SQLite3Table(tableName, databaseColumns,
                            tableType.contentEquals("temp_table") ? SQLite3Table.TableKind.TEMP
                                    : SQLite3Table.TableKind.MAIN,
                            withoutRowid, isView, isVirtual, isReadOnly);
                    if (isRowIdTable(withoutRowid, isView, isVirtual)) {
                        SQLite3Column rowid = new SQLite3Column("rowid", SQLite3DataType.INTEGER, true, true, null, true, true);
                        t.addRowid(rowid);
                        rowid.setTable(t);
                    }
                    for (SQLite3Column c : databaseColumns) {
                        c.setTable(t);
                    }
                    databaseTables.add(t);
                }
            } catch (SQLException e) {
                // ignore
            }
            // obtain indexes for each non-virtual table
            for (SQLite3Table table : databaseTables) {
                SQLite3Index.getTableIndexes(con, table);
            }

            // obtain non-temporary trigger
            try (ResultSet rs = s.executeQuery("SELECT name FROM main.sqlite_master where type in ('trigger')")) {
                while (rs.next()) {
                    String triggerName = rs.getString("name");
                    SQLite3Trigger trigger = new SQLite3Trigger(triggerName, false);
                    triggers.add(trigger);
                }
            } catch (SQLException ignored) {
            }

            // obtain temporary trigger
            try (ResultSet rs = s.executeQuery("SELECT name FROM temp.sqlite_master where type in ('trigger')")) {
                while (rs.next()) {
                    String triggerName = rs.getString("name");
                    SQLite3Trigger trigger = new SQLite3Trigger(triggerName, true);
                    triggers.add(trigger);
                }
            } catch (SQLException ignored) {
            }
        }

        return new SQLite3Schema(databaseTables, triggers);
    }

    // https://www.sqlite.org/rowidtable.html
    private static boolean isRowIdTable(boolean withoutRowid, boolean isView, boolean isVirtual) {
        return !isView && !isVirtual && !withoutRowid;
    }

    public SQLite3Table getRandomVirtualTable() {
        return getRandomTable(SQLite3Table::isVirtual);
    }

    public SQLite3Tables getTables() {
        return new SQLite3Tables(getDatabaseTables());
    }

    public SQLite3Tables getRandomTableNonEmptyTables() {
        if (getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException();
        }
        return new SQLite3Tables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public SQLite3Table getRandomTableNoViewNoVirtualTable() {
        return Randomly.fromList(getDatabaseTablesWithoutViewsWithoutVirtualTables());
    }

    public List<SQLite3Table> getDatabaseTablesWithoutViewsWithoutVirtualTables() {
        return getDatabaseTables().stream().filter(t -> !t.isView() && !t.isVirtual()).collect(Collectors.toList());
    }

    public String getFreeVirtualTableName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String tableName = String.format("vt%d", i++);
            if (getDatabaseTables().stream().noneMatch(t -> t.getName().equalsIgnoreCase(tableName))) {
                return tableName;
            }
        } while (true);

    }

    public String getFreeRtreeTableName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String tableName = String.format("rt%d", i++);
            if (getDatabaseTables().stream().noneMatch(t -> t.getName().equalsIgnoreCase(tableName))) {
                return tableName;
            }
        } while (true);

    }

}
