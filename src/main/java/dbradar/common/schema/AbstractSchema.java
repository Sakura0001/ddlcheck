package dbradar.common.schema;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import dbradar.GlobalState;
import dbradar.IgnoreMeException;
import dbradar.Randomly;

public abstract class AbstractSchema<G extends GlobalState, T extends AbstractTable<?, ?, G>, TR extends AbstractTrigger> {

    private List<T> databaseTables;
    private List<TR> triggers;

    public AbstractSchema(List<T> databaseTables, List<TR> triggers) {
        this.databaseTables = databaseTables;
        this.triggers = triggers;
    }

    /**
     * Return all tables and views
     */
    public List<T> getDatabaseTables() {
        return databaseTables;
    }

    /**
     * Return all tables
     */
    public List<T> getDatabaseTablesWithoutViews() {
        return getDatabaseTables().stream().filter(t -> !t.isView()).collect(Collectors.toList());
    }

    public T getRandomTable(Predicate<T> predicate) {
        List<T> relevantTables = getDatabaseTables().stream().filter(predicate).collect(Collectors.toList());
        if (relevantTables.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(relevantTables);
    }

    /**
     * Return a random table without view
     */
    public T getRandomTable() {
        return getRandomTable(t -> !t.isView());
    }

    /**
     * Return a random non-temp table without view
     */
    public T getRandomTableWithoutTemp() {
        return getRandomTable(t -> !t.isView() && !t.isTemporary());
    }

    /**
     * Return a random table without view
     */
    public T getRandomTableWithoutView() {
        return getRandomTable(t -> !t.isView());
    }

    public String getFreeTableName() {
        int i = getDatabaseTables().size();
        do {
            String tableName = String.format("t%d", i++);
            String virtualName = "v" + tableName;
            if (getDatabaseTables().stream().noneMatch(t -> t.getName().equalsIgnoreCase(tableName))
                    && getDatabaseTables().stream().noneMatch(t -> t.getName().equalsIgnoreCase(virtualName))) {
                return tableName;
            }
        } while (true);
    }

    public List<T> getViews() {
        return getDatabaseTables().stream().filter(t -> t.isView()).collect(Collectors.toList());
    }

    public T getRandomView() {
        return getRandomTable(t -> t.isView());
    }

    public String getFreeViewName() {
        int i = getViews().size();
        do {
            String viewName = String.format("v%d", i++);
            if (getViews().stream().noneMatch(t -> t.getName().contentEquals(viewName))) {
                return viewName;
            }
        } while (true);
    }

    public boolean containsTableWithZeroRows(GlobalState globalState) {
        return getDatabaseTables().stream().anyMatch(t -> t.getNrRows(globalState) == 0);
    }

    public String getFreeIndexName() {
        int i = getDatabaseTables().size();
        do {
            String indexName = String.format("i%d", i++);
            boolean indexNameFound = false;
            for (T table : getDatabaseTables()) {
                if (table.getIndexes().stream().anyMatch(ind -> ind.getName().contentEquals(indexName))) {
                    indexNameFound = true;
                    break;
                }
            }
            if (!indexNameFound) {
                return indexName;
            }
        } while (true);
    }

    public List<TR> getTriggers() {
        return triggers;
    }

    public TR getRandomTrigger() {
        if (getTriggers().isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(getTriggers());
    }

    public String getFreeTriggerName() {
        int i = getTriggers().size();
        do {
            String triggerName = String.format("tr%d", i++);
            if (getTriggers().stream().noneMatch(t -> t.getName().contentEquals(triggerName))) {
                return triggerName;
            }
        } while (true);
    }
}
