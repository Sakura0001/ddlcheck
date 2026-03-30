package dbradar.common.query.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import grammar.Production;
import grammar.Token;
import dbradar.common.schema.AbstractTable;
import dbradar.common.schema.AbstractTableColumn;

public class QueryContext {

    // TODO No one uses this property?
    private Map<Token, Integer> nonTermDepth = new HashMap<>();

    // Context information for UPDATE
    private int valueIndex = 0;

    // Context information for SELECT
    private List<AbstractTableColumn<?, ?>> currentColumns = new ArrayList<>();
    private Queue<AbstractTableColumn<?, ?>> returnedColumns = new LinkedList<>();
    private List<String> availableColumnAliases = new ArrayList<>();

    private List<AbstractTableColumn<?, ?>> selectedCols = new ArrayList<>();

    private List<ColumnReferenceFiller> fillers = new ArrayList<>();
    private List<AbstractTable<?, ?, ?>> curTables = new ArrayList<>();

    // A common data structure for storing properties in different DBMSs
    private Map<String, Object> properties = new HashMap<String, Object>();

    public Map<Token, Integer> getNonTermDepth() {
        return nonTermDepth;
    }

    // TODO No one uses these methods?
    private Map<Production, Integer> productionDepth = new HashMap<>();
    private List<Production> visitedProds = new ArrayList<>();

    public Map<Production, Integer> getProductionDepth() {
        return productionDepth;
    }

    public List<Production> getVisitedProds() {
        return visitedProds;
    }

    public List<SimplePair<AbstractTableColumn<?, ?>,AbstractTableColumn<?, ?>>> foreignKeyCols = new ArrayList<>();
    public  SimplePair<AbstractTableColumn<?, ?>,AbstractTableColumn<?, ?>> selectedForeignKeyPair = null;
    public static class SimplePair<K, V> {
        private final K key;
        private final V value;

        public SimplePair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }

    public AbstractTableColumn<?, ?> getColumn() {
        if (returnedColumns.isEmpty()) {
            return null;
        } else {
            return returnedColumns.poll();
        }
    }

    public void fillColumnReferences() {
        int size = fillers.size();
        // remove concurrent modification, do not use for each, currently such fillers
        // may fail
        for (int i = 0; i < size; i++) {
            fillers.get(i).fill();
        }
        fillers.clear();
    }

    public List<ColumnReferenceFiller> getFillers() {
        return fillers;
    }

    public void addCurrentColumn(AbstractTableColumn<?, ?> column) {
        currentColumns.add(column);
    }

    public List<AbstractTableColumn<?, ?>> getCurrentColumns() {
        return currentColumns;
    }

    public void addReturnedColumn(AbstractTableColumn<?, ?> column) {
        returnedColumns.add(column);
    }

    public Queue<AbstractTableColumn<?, ?>> getReturnedColumns() {
        return returnedColumns;
    }

    public void addColumnAlias(String alias) {
        availableColumnAliases.add(alias);
    }

    public List<String> getAvailableColumnAliases() {
        return availableColumnAliases;
    }

    public void addFiller(ColumnReferenceFiller filler) {
        fillers.add(filler);
    }

    public void addSelectedColumn(AbstractTableColumn<?, ?> column) {
        selectedCols.add(column);
    }

    public List<AbstractTableColumn<?, ?>> getSelectedColumns() {
        return selectedCols;
    }

    public int getValueIndex() {
        return valueIndex;
    }

    public void increaseValueIndex() {
        valueIndex++;
    }

    public void addSelectedTable(AbstractTable<?, ?, ?> table) {
        curTables.add(table);
    }

    public List<AbstractTable<?, ?, ?>> getSelectedTables() {
        return curTables;
    }

    public void setProperty(String key, Object value) {
        properties.put(key, value);
    }

    public Object getProperty(String key) {
        return properties.get(key);
    }

    public void addForeignPair(AbstractTableColumn<?, ?> a,AbstractTableColumn<?, ?>b){
        foreignKeyCols.add(new SimplePair<AbstractTableColumn<?, ?>,AbstractTableColumn<?, ?>>(a,b));
    }
}
