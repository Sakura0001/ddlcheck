package dbradar.common.query.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;

import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.SQLGlobalState;
import grammar.Token;
import grammar.Token.TokenType;
import dbradar.common.query.generator.data.Generator;
import dbradar.common.query.generator.data.GeneratorRegister;
import dbradar.common.schema.AbstractTable;
import dbradar.common.schema.AbstractTableColumn;

public class KeyFuncManager extends AbstractKeyFuncManager {

    private Stack<QueryContext> contextStack = new Stack<>();

    protected int columnAlias = 1;
    protected int tableAlias = 1;

    public KeyFuncManager(SQLGlobalState globalState) {
        super(globalState);
        initKeyFuncs();
    }

    private void initKeyFuncs() {
        // Register basic data types as key functions.
        Map<String, Generator> generators = GeneratorRegister.getBasicGenerators(globalState);
        for (String dataType : generators.keySet()) {
            keyFuncMap.put("_" + dataType, new KeyFunc() {
                @Override
                public void generateAST(ASTNode parent) {
                    String value = generators.get(dataType).generate(globalState);
                    ASTNode valueNode = new ASTNode(new Token(TokenType.TERMINAL, value));
                    parent.addChild(valueNode);
                }
            });
        }

        keyFuncMap.put(StartContext.KEY, new StartContext());
        keyFuncMap.put(EndContext.KEY, new EndContext());
        keyFuncMap.put(ReturnContext.KEY, new ReturnContext());

        keyFuncMap.put(TableKeyFunc.KEY, new TableKeyFunc());
        keyFuncMap.put(TempTableKeyFunc.KEY, new TempTableKeyFunc());
        keyFuncMap.put(TableWithoutTempKeyFunc.KEY, new TableWithoutTempKeyFunc());
        keyFuncMap.put(TableWithOutViewKeyFunc.KEY, new TableWithOutViewKeyFunc());
        keyFuncMap.put(ReferenceTableKeyFunc.KEY, new ReferenceTableKeyFunc());
        keyFuncMap.put(NewTableNameKeyFunc.KEY, new NewTableNameKeyFunc());
        keyFuncMap.put(TableAliasKeyFunc.KEY, new TableAliasKeyFunc());

        keyFuncMap.put(ColumnKeyFunc.KEY, new ColumnKeyFunc());
        keyFuncMap.put(DistinctColumnKeyFunc.KEY, new DistinctColumnKeyFunc());
        keyFuncMap.put(DropColumnKeyFunc.KEY, new DropColumnKeyFunc());
        keyFuncMap.put(AllColumnKeyFunc.KEY, new AllColumnKeyFunc());
        keyFuncMap.put(NewColumnNameKeyFunc.KEY, new NewColumnNameKeyFunc());

        keyFuncMap.put(ColumnAliasKeyFunc.KEY, new ColumnAliasKeyFunc());

        keyFuncMap.put(UsingKeyFunc.KEY, new UsingKeyFunc());

        keyFuncMap.put(NewIndexKeyFunc.KEY, new NewIndexKeyFunc());
        keyFuncMap.put(ViewKeyFunc.KEY, new ViewKeyFunc());

        keyFuncMap.put(InsertValueKeyFunc.KEY, new InsertValueKeyFunc());
        keyFuncMap.put(AssignValueKeyFunc.KEY, new AssignValueKeyFunc());

        keyFuncMap.put(NewTriggerNameKeyFunc.KEY, new NewTriggerNameKeyFunc());

        keyFuncMap.put(NewViewNameKeyFunc.KEY, new NewViewNameKeyFunc());
    }

    public void addFiller(ColumnReferenceFiller filler) {
        currentContext.addFiller(filler);
    }

    public int addTableAlias() {
        return tableAlias++;
    }

    /**
     * This key function is used to create a new context when a SQL statement
     * starts. We use this function to initialize the context to store necessary
     * information when generating a statement.
     * <p>
     * This key function is used for SELECT, especially nested sub-queries.
     * <p>
     * For example, _context SELECT _column as _alias FROM _table _end
     */
    private class StartContext implements KeyFunc {

        public static final String KEY = "_context";

        @Override
        public void generateAST(ASTNode parent) {
            QueryContext newContext = new QueryContext();
            if (currentContext != null) {
                contextStack.push(currentContext);
            }
            currentContext = newContext;
        }
    }

    /**
     * This key function is used to indicate that a context ends. We use this
     * function to finalize the current context.
     * <p>
     * For example, _context SELECT _column as _alias FROM _table _return
     */
    private class ReturnContext implements KeyFunc {

        public static final String KEY = "_return";

        @Override
        public void generateAST(ASTNode parent) {
            currentContext.fillColumnReferences();

            QueryContext lastContext = currentContext;
            if (!contextStack.isEmpty()) {
                currentContext = contextStack.pop();
            } else {
                currentContext = null;
            }

            if (currentContext != null) {
                if (!lastContext.getReturnedColumns().isEmpty())
                    currentContext.getCurrentColumns().addAll(lastContext.getReturnedColumns());
                    /*
                     * when _context (select) _return select : ... _context single_select3 _return
                     * compound_select_more+ multiple contexts make returnedColumns null
                     */
                else
                    currentContext.getCurrentColumns().addAll(lastContext.getCurrentColumns());
            }
        }
    }

    private class EndContext implements KeyFunc {

        public static final String KEY = "_end";

        @Override
        public void generateAST(ASTNode parent) {
            currentContext.fillColumnReferences();
            if (!contextStack.isEmpty()) {
                currentContext = contextStack.pop();
            } else {
                currentContext = null;
            }
        }
    }

    /**
     * This key function is used to fetch an existing table. For example, SELECT
     * _column as _alias FROM _table
     */
    private class TableKeyFunc implements KeyFunc {

        public static final String KEY = "_table";

        @Override
        public void generateAST(ASTNode parent) {

            AbstractTable<?, ?, ?> table;
            try {
                table = globalState.getSchema().getRandomTable();
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There are not available tables for _table.");
            }

            // If a selected table is duplicated, we create a table alias for it, e.g., t1
            // as ta1
            if (currentContext.getSelectedTables().contains(table)) {
                List<AliasTableColumn<?>> aliasColumns = new ArrayList<>();
                for (AbstractTableColumn<?, ?> column : table.getColumns()) {
                    AliasTableColumn<?> aliasCol = new AliasTableColumn<>(column.getName(), null, null);
                    aliasColumns.add(aliasCol);
                }
                AliasTable<?, ?, ?> aliasTable = new AliasTable<>("ta" + tableAlias++, aliasColumns,
                        table.getIndexes());
                for (AliasTableColumn<?> column : aliasColumns) {
                    column.setTable(aliasTable);
                }
                currentContext.getCurrentColumns().addAll(aliasColumns);
                ASTNode tableNode = new ASTNode(new Token(TokenType.TERMINAL, table.getName()));
                parent.addChild(tableNode);
                parent.addChild(new ASTNode(new Token(TokenType.TERMINAL, "AS", true)));
                ASTNode aliasNode = new ASTNode(new Token(TokenType.KEYWORD, "_table_alias", true));
                parent.addChild(aliasNode);
                aliasNode.addChild(new ASTNode(new Token(TokenType.TERMINAL, aliasTable.getName())));
            } else {
                currentContext.addSelectedTable(table);
                currentContext.getCurrentColumns().addAll(table.getColumns());
                ASTNode tableNode = new ASTNode(new Token(TokenType.TERMINAL, table.getName()));
                parent.addChild(tableNode);
            }
        }
    }

    /*
     * This key function is used to return a reference table used in foreign key
     * constraint. For example, FOREIGN KEY (_column) REFERENCES _reference_table
     */
    private class ReferenceTableKeyFunc implements KeyFunc {

        public static final String KEY = "_reference_table";

        @Override
        public void generateAST(ASTNode parent) {
            AbstractTable<?, ?, ?> table;
            try {
                table = globalState.getSchema().getRandomTable(AbstractTable::hasPrimaryKey);
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There are not available tables for _reference_table.");
            }

            AbstractTableColumn<?, ?> col = table.getPrimaryColumn();

            ASTNode tblNode = new ASTNode(new Token(TokenType.TERMINAL, table.getName()));
            ASTNode leftPara = new ASTNode(new Token(TokenType.TERMINAL, "("));
            ASTNode colNode = new ASTNode(new Token(TokenType.TERMINAL, col.getName()));
            ASTNode rightPara = new ASTNode(new Token(TokenType.TERMINAL, ")"));

            parent.addChild(tblNode);
            parent.addChild(leftPara);
            parent.addChild(colNode);
            parent.addChild(rightPara);
        }

    }

    /**
     * This key function is used to return a temporary table. For example,
     * DROP TEMPORARY TABLE _temp_table
     */
    private class TempTableKeyFunc implements KeyFunc {
        public static final String KEY = "_temp_table";

        @Override
        public void generateAST(ASTNode parent) {
            String tempTableName;
            try {
                AbstractTable<?, ?, ?> table = globalState.getSchema().getRandomTable(AbstractTable::isTemporary);
                tempTableName = table.getName();
            } catch (IgnoreMeException e) {
                throw new QueryGenerationException("There is not available temporary table for _temp_table");
            }
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, tempTableName)));
        }
    }

    /**
     * This key function is used to fetch an existing table that is not temp. For
     * example, SELECT _column as _alias FROM _table_without_temp
     * INHERITS (_table_without_temp)
     */
    private class TableWithoutTempKeyFunc implements KeyFunc {

        public static final String KEY = "_table_without_temp";

        @Override
        public void generateAST(ASTNode parent) {
            AbstractTable<?, ?, ?> table;
            try {
                table = globalState.getSchema().getRandomTableWithoutTemp();
            } catch (IgnoreMeException e) {
                throw new IgnoreMeException("There are not available tables for _table_without_temp.");
            }

            // If a selected table is duplicated, we create a table alias for it, e.g., t1
            // as ta1
            if (currentContext.getSelectedTables().contains(table)) {
                List<AliasTableColumn<?>> aliasColumns = new ArrayList<>();
                for (AbstractTableColumn<?, ?> column : table.getColumns()) {
                    AliasTableColumn<?> aliasCol = new AliasTableColumn<>(column.getName(), null, null);
                    aliasColumns.add(aliasCol);
                }
                AliasTable<?, ?, ?> aliasTable = new AliasTable<>("ta" + addTableAlias(), aliasColumns,
                        table.getIndexes());
                for (AliasTableColumn<?> column : aliasColumns) {
                    column.setTable(aliasTable);
                }
                currentContext.getCurrentColumns().addAll(aliasColumns);
                ASTNode tableNode = new ASTNode(new Token(Token.TokenType.TERMINAL, table.getName()));
                parent.addChild(tableNode);
                parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, "AS", true)));
                ASTNode aliasNode = new ASTNode(new Token(Token.TokenType.KEYWORD, "_table_alias", true));
                parent.addChild(aliasNode);
                aliasNode.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, aliasTable.getName())));
            } else {
                currentContext.addSelectedTable(table);
                currentContext.getCurrentColumns().addAll(table.getColumns());
                ASTNode tableNode = new ASTNode(new Token(Token.TokenType.TERMINAL, table.getName()));
                parent.addChild(tableNode);
            }
        }
    }

    /**
     * This key function is used to fetch an existing table that is not a view. For example,
     * TRUNCATE TABLE _table_without_view
     */
    private class TableWithOutViewKeyFunc implements KeyFunc {

        public static final String KEY = "_table_without_view";

        @Override
        public void generateAST(ASTNode parent) {
            AbstractTable<?, ?, ?> table;
            try {
                table = globalState.getSchema().getRandomTableWithoutView();
            } catch (IgnoreMeException e) {
                throw new IgnoreMeException("There are not available tables for _table_without_view.");
            }

            ASTNode tableNode = new ASTNode(new Token(Token.TokenType.TERMINAL, table.getName()));
            parent.addChild(tableNode);
        }
    }


    /**
     * This key function is used to fetch a new table name. For example, CREATE
     * TABLE _new_table_name (c1 INT)
     */
    private class NewTableNameKeyFunc implements KeyFunc {

        public static final String KEY = "_new_table_name";

        @Override
        public void generateAST(ASTNode parent) {
            String tableName = globalState.getSchema().getFreeTableName();
            ASTNode tableNode = new ASTNode(new Token(Token.TokenType.TERMINAL, tableName));
            parent.addChild(tableNode);
        }
    }

    /**
     * This key function is used to return an alias name for a table. For example,
     * SELECT c1 FROM (SELECT c1 FROM t1) _table_alias
     * Every derived table must have its own alias
     */
    private class TableAliasKeyFunc implements KeyFunc {

        public static final String KEY = "_table_alias";

        @Override
        public void generateAST(ASTNode parent) {
            String aliasName = "ta" + tableAlias++;
            List<AbstractTableColumn<?, ?>> columns = currentContext.getCurrentColumns();
            List<AliasTableColumn<?>> aliasColumns = new ArrayList<>();
            for (AbstractTableColumn<?, ?> column : columns) {
                AliasTableColumn<?> aliasCol = new AliasTableColumn<>(column.getName(), null, null);
                aliasColumns.add(aliasCol);
            }
            AliasTable<?, ?, ?> table = new AliasTable<>(aliasName, columns, null); // todo obtain index
            for (AliasTableColumn<?> column : aliasColumns) {
                column.setTable(table);
            }
            currentContext.getCurrentColumns().clear();
            currentContext.getCurrentColumns().addAll(aliasColumns);
            ASTNode aliasNode = new ASTNode(new Token(Token.TokenType.TERMINAL, aliasName));
            parent.addChild(aliasNode);
        }
    }


    /**
     * This key function is used to fetch an existing column. For example, SELECT
     * _column as _column_alias FROM _table
     */
    private class ColumnKeyFunc implements KeyFunc {

        public static final String KEY = "_column";

        @Override
        public void generateAST(ASTNode parent) {
            List<AbstractTableColumn<?, ?>> columns = new ArrayList<>(currentContext.getCurrentColumns());

            if (columns.isEmpty()) {
                currentContext.addFiller(new ColumnReferenceFiller(parent, this));
            } else {
                // Avoid CREATE TABLE (c1 INT, c2 INT GENERATED AS (c2 + 1))
                // TODO This is a tricky solution, we need to find a better one
                AbstractTableColumn<?, ?> lastColumn = columns.get(columns.size() - 1);
                ASTNode pNode = parent;
                while (pNode != null) {
                    String symbol = pNode.getToken().getValue();
                    if (symbol.equals("generated_constraint")) { // generated_constraint is the symbol in grammar
                        // The current column definition is not done
                        columns.remove(lastColumn);
                        break;
                    }
                    pNode = pNode.getParent();
                }

                // generated column cannot be used in primary key
                // jiansen: does not work, because the property of generated column is obtained
                // from the database
                boolean usedInPrimaryKey = false;
                pNode = parent;
                while (pNode != null) {
                    String symbol = pNode.getToken().getValue();
                    if (symbol.equals("primary_key_table_constraint")) {
                        usedInPrimaryKey = true;
                        break;
                    }
                    pNode = pNode.getParent();
                }
                if (usedInPrimaryKey) {
                    for (int i = 0; i < columns.size(); i++) {
                        AbstractTableColumn<?, ?> column = columns.get(i);
                        if (column.isGenerated()) { // todo does not work, try to fetch it in the ast
                            columns.remove(column);
                            i--;
                        }
                    }
                }

                if (columns.isEmpty()) {
                    throw new QueryGenerationException("There is not suitable column for _column");
                }

                AbstractTableColumn<?, ?> col = Randomly.fromList(columns);
                currentContext.addSelectedColumn(col);
                ASTNode columnNode = new ASTNode(new Token(TokenType.TERMINAL, getColumnName(col)));
                parent.addChild(columnNode);
            }
        }
    }

    /**
     * This key function is used to fetch a distinct column from current column.
     * <p>
     * This function is used for UPDATE and INSERT, which does not support duplicate
     * column lists.
     * <p>
     * For example, UPDATE _table SET _distinct_column = _value, distinct_column =
     * _value
     * For example, CREATE INDEX _new_index_name ON _table (index_table_column)
     * INCLUDE (_distinct_column)
     */
    private class DistinctColumnKeyFunc implements KeyFunc {

        public static final String KEY = "_distinct_column";

        @Override
        public void generateAST(ASTNode parent) {
            List<AbstractTableColumn<?, ?>> columns = currentContext.getCurrentColumns();
            List<AbstractTableColumn<?, ?>> distinctCols = columns.stream()
                    .filter(element -> !currentContext.getSelectedColumns().contains(element) && !element.isGenerated())
                    .collect(Collectors.toList());
            if (currentContext.getCurrentColumns().isEmpty()) {
                currentContext.addFiller(new ColumnReferenceFiller(parent, this));
            }
            if (!distinctCols.isEmpty()) {
                AbstractTableColumn<?, ?> col = Randomly.fromList(distinctCols);
                currentContext.addSelectedColumn(col);
                ASTNode colNode = new ASTNode(new Token(TokenType.TERMINAL, getColumnName(col)));
                parent.addChild(colNode);
            } else {
                throw new QueryGenerationException("There is no more column for _distinct_column.");
            }
        }
    }

    /**
     * This key function is used to fetch an existing column to drop, and make sure
     * there are still other columns in the table after dropping. For example, ALTER
     * TABLE _table DROP COLUMN? _drop_column
     */
    private class DropColumnKeyFunc implements KeyFunc {

        public static final String KEY = "_drop_column";

        @Override
        public void generateAST(ASTNode parent) {
            List<AbstractTableColumn<?, ?>> columns = new ArrayList<>();
            for (AbstractTableColumn<?, ?> col : currentContext.getCurrentColumns()) {
                if (!col.getName().equals("rowid") && !col.isPrimaryKey()) {
                    columns.add(col);
                }
            }
            if (columns.size() < 2) {
                throw new QueryGenerationException("There are not available columns to drop.");
            } else {
                AbstractTableColumn<?, ?> col = Randomly.fromList(columns);
                currentContext.addSelectedColumn(col);
                ASTNode columnNode = new ASTNode(new Token(TokenType.TERMINAL, getColumnName(col)));
                parent.addChild(columnNode);
            }
        }
    }

    /**
     * This key function is used to return all insertable columns. For example,
     * INSERT INTO t1 (_insert_columns)
     */
    private class AllColumnKeyFunc implements KeyFunc {

        public static final String KEY = "_insert_columns";

        @Override
        public void generateAST(ASTNode parent) {
            List<AbstractTableColumn<?, ?>> columns = currentContext.getCurrentColumns();
            if (columns.isEmpty()) {
                currentContext.addFiller(new ColumnReferenceFiller(parent, this));
            } else {
                List<AbstractTableColumn<?, ?>> candidateCols = columns.stream().filter(c -> !c.isGenerated())
                        .collect(Collectors.toList());
                currentContext.setProperty("insertable_columns", new ArrayList<>(candidateCols));
                for (int i = 0; i < candidateCols.size(); i++) {
                    AbstractTableColumn<?, ?> col = candidateCols.get(i);
                    currentContext.addReturnedColumn(col);
                    ASTNode colNode = new ASTNode(new Token(TokenType.TERMINAL, getColumnName(col)));
                    parent.addChild(colNode);
                    if (i != candidateCols.size() - 1) {
                        parent.addChild(new ASTNode(new Token(TokenType.TERMINAL, ",")));
                    }
                }
            }
        }
    }

    /**
     * This key function is used to return a new column. For example, ALTER TABLE t1
     * RENAME _column TO _new_column_name, CREATE TABLE t1 (_new_column_name INT,
     * _new_column_name DOUBLE)
     */
    private class NewColumnNameKeyFunc implements KeyFunc {

        public static final String KEY = "_new_column_name";

        @Override
        public void generateAST(ASTNode parent) {
            String colName;
            if (!currentContext.getSelectedTables().isEmpty()) {
                // For ALTER TABLE
                AbstractTable<?, ?, ?> curTable = currentContext.getSelectedTables().get(0);
                colName = curTable.getFreeColumnName();
            } else {
                // For CREATE TABLE
                colName = "c" + (currentContext.getCurrentColumns().size() + 1);
                AliasTableColumn<?> aliasCol = new AliasTableColumn<>(colName, null, null);
                currentContext.addCurrentColumn(aliasCol);
            }
            parent.addChild(new ASTNode(new Token(TokenType.TERMINAL, colName)));
        }
    }

    /**
     * This key function is used to create an alias for a column. We use this key
     * function to create aliases to avoid potential naming conflicts, reference to
     * the expression in a SELECT field.
     * <p>
     * For example, SELECT _column as _column_alias, (_column + 1) as _column_alias
     * FROM _table
     */
    private class ColumnAliasKeyFunc implements KeyFunc {

        public static final String KEY = "_column_alias";

        @Override
        public void generateAST(ASTNode parent) {
            String aliasName = "ca" + columnAlias++;
            currentContext.addReturnedColumn(new AliasTableColumn<>(aliasName, null, null));
            currentContext.addColumnAlias(aliasName);
            ASTNode aliasNode = new ASTNode(new Token(TokenType.TERMINAL, aliasName));
            parent.addChild(aliasNode);
        }
    }

    /**
     * This key function is used for generating the column of using for JOIN For
     * example, SELECT * FROM t1 JOIN t2 USING(c1) _using_column only peek one
     * common column from each table source of JOIN
     */
    private class UsingKeyFunc implements KeyFunc {

        public static final String KEY = "_using_column";

        @Override
        public void generateAST(ASTNode parent) {
            if (currentContext.getSelectedTables().size() < 2) { // at least two tables
                throw new QueryGenerationException("No common column can be used in using clause.");
            }

            AbstractTable<?, ?, ?> left = currentContext.getSelectedTables().get(0);
            AbstractTable<?, ?, ?> right = currentContext.getSelectedTables().get(1);
            List<AbstractTableColumn<?, ?>> commonCols = new ArrayList<>();
            for (AbstractTableColumn<?, ?> lCol : left.getColumns()) {
                for (AbstractTableColumn<?, ?> rCol : right.getColumns()) {
                    if (lCol.getName().equals(rCol.getName())) {
                        commonCols.add(lCol);
                    }
                }
            }

            if (commonCols.isEmpty()) {
                // CREATE TABLE t1 (c1 INT);
                // CREATE TABLE t2 (c2 INT);
                throw new QueryGenerationException("No common column can be used in using clause.");
            }

            String colName = Randomly.fromList(commonCols).getName();
            ASTNode colNode = new ASTNode(new Token(TokenType.TERMINAL, colName));
            parent.addChild(colNode);

        }
    }

    /**
     * This key function is used to return an existing view. For example, DROP VIEW
     * _view
     */
    private class ViewKeyFunc implements KeyFunc {
        public static final String KEY = "_view";

        @Override
        public void generateAST(ASTNode parent) {
            String viewName;
            try {
                viewName = globalState.getSchema().getRandomView().getName();
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There are no available views for _view.");
            }

            parent.addChild(new ASTNode(new Token(TokenType.TERMINAL, viewName)));
        }

    }


    /**
     * This key function is used to return a new index. For example, CREATE INDEX
     * _new_index_name ON t1 (c1)
     */
    private class NewIndexKeyFunc implements KeyFunc {

        public static final String KEY = "_new_index_name";

        @Override
        public void generateAST(ASTNode parent) {
            String indexName = globalState.getSchema().getFreeIndexName();
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, indexName)));
        }
    }


    /**
     * This key function is used to return values for all insertable columns.
     * <p>
     * For example, INSERT INTO t1 VALUES (_insert_values)
     */
    private class InsertValueKeyFunc implements KeyFunc {

        public static final String KEY = "_insert_values";

        @Override
        public void generateAST(ASTNode parent) {
            int colSize = currentContext.getReturnedColumns().size();
            for (int i = 0; i < colSize; i++) {
                AbstractTableColumn<?, ?> col = currentContext.getReturnedColumns().poll();
                Generator generator = GeneratorRegister.getGenerator(col, globalState);
                String value = generator.generate(globalState);
                while (col.isNotNull() && value.equals("null")) {
                    value = generator.generate(globalState);
                }
                ASTNode valueNode = new ASTNode(new Token(TokenType.TERMINAL, value));
                parent.addChild(valueNode);
                if (i != colSize - 1) {
                    parent.addChild(new ASTNode(new Token(TokenType.TERMINAL, ",")));
                }
            }
        }
    }

    /**
     * This key function is used to return a value for the updated column.
     * <p>
     * For example, UPDATE t1 SET c1 = _value
     */
    private class AssignValueKeyFunc implements KeyFunc {

        public static final String KEY = "_value";

        @Override
        public void generateAST(ASTNode parent) {
            int index = currentContext.getValueIndex();

            AbstractTableColumn<?, ?> column = currentContext.getSelectedColumns().get(index);
            currentContext.increaseValueIndex();
            if (column != null) {
                Generator generator = GeneratorRegister.getGenerator(column, globalState);
                String value = generator.generate(globalState);
                while (column.isNotNull() && value.equals("null")) {
                    value = generator.generate(globalState);
                }
                ASTNode valueNode = new ASTNode(new Token(TokenType.TERMINAL, value));
                parent.addChild(valueNode);
            }
        }
    }

    /**
     * This key function is used to return a new trigger name For example, CREATE
     * TRIGGER _new_trigger_name
     */
    private class NewTriggerNameKeyFunc implements KeyFunc {
        public static final String KEY = "_new_trigger_name";

        @Override
        public void generateAST(ASTNode parent) {
            String triggerName = globalState.getSchema().getFreeTriggerName();
            parent.addChild(new ASTNode(new Token(TokenType.TERMINAL, triggerName)));
        }
    }

    /**
     * This key function is used to return a new view name For example, CREATE VIEW
     * _new_view_name (_new_column) AS select
     */
    private class NewViewNameKeyFunc implements KeyFunc {
        public static final String KEY = "_new_view_name";

        @Override
        public void generateAST(ASTNode parent) {
            String viewName = globalState.getSchema().getFreeViewName();
            ASTNode tableNode = new ASTNode(new Token(Token.TokenType.TERMINAL, viewName));
            parent.addChild(tableNode);
        }
    }

    protected String getColumnName(AbstractTableColumn<?, ?> column) {
        for (AbstractTableColumn<?, ?> col : currentContext.getCurrentColumns()) {
            if (column != col && column.getName().equals(col.getName())) { // Find naming conflict
                return column.getFullQualifiedName();
            }
        }
        return column.getName();
    }
}
