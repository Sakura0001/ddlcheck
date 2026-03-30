package dbradar.mysql;

import dbradar.IgnoreMeException;
import dbradar.Randomly;
import grammar.Token;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.query.generator.AliasTable;
import dbradar.common.query.generator.AliasTableColumn;
import dbradar.common.query.generator.ColumnReferenceFiller;
import dbradar.common.query.generator.KeyFunc;
import dbradar.common.query.generator.KeyFuncManager;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.common.query.generator.data.Generator;
import dbradar.common.query.generator.data.GeneratorRegister;
import dbradar.common.query.generator.data.IntGenerator;
import dbradar.common.schema.AbstractTable;
import dbradar.common.schema.AbstractTableColumn;
import dbradar.mysql.schema.MySQLSchema;
import dbradar.mysql.schema.MySQLSchema.MySQLIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class MySQLKeyFuncManager extends KeyFuncManager {

    public MySQLKeyFuncManager(MySQLGlobalState globalState) {
        super(globalState);

        keyFuncMap.put(TableKeyFunc.KEY, new TableKeyFunc());
        keyFuncMap.put(DropIndexKeyFunc.KEY, new DropIndexKeyFunc());
        keyFuncMap.put(IndexKeyFunc.KEY, new IndexKeyFunc());
        keyFuncMap.put(DistinctKeyKeyFunc.KEY, new DistinctKeyKeyFunc());
        keyFuncMap.put(ForeignKeyColumnKeyFunc.KEY, new ForeignKeyColumnKeyFunc());
        keyFuncMap.put(InsertSelectSameTableKeyFunc.KEY, new InsertSelectSameTableKeyFunc());
        keyFuncMap.put(ExistingColumnAliasKeyFunc.KEY, new ExistingColumnAliasKeyFunc());
        keyFuncMap.put(InsertDuplicateUpdateColumnKeyFunc.KEY, new InsertDuplicateUpdateColumnKeyFunc());
        keyFuncMap.put("_insert_values", new StressInsertValueKeyFunc());
        keyFuncMap.put("_value", new StressAssignValueKeyFunc());
    }

    /**
     * This key function is used to fetch an existing table. For example, SELECT
     * _column as _alias FROM _table_select
     * Note that this is a special for select statement, in which every derived table must have an alias name
     */
    private class TableKeyFunc implements KeyFunc {

        public static final String KEY = "_table_select";

        @Override
        public void generateAST(ASTNode parent) {

            AbstractTable<?, ?, ?> table;
            try {
                table = globalState.getSchema().getRandomTable();
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There are not available tables for _table.");
            }

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
     * This key function is used to return an existing index. For example, DROP
     * INDEX _index
     */
    private class DropIndexKeyFunc implements KeyFunc {
        public static final String KEY = "_drop_index";

        @Override
        public void generateAST(ASTNode parent) {
            MySQLIndex index;
            try {
                index = ((MySQLSchema) globalState.getSchema()).getRandomIndex();
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There are no available indexes for _index.");
            }
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, index.getName())));
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, " ON ")));
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, index.getTableName())));
        }

    }

    /**
     * This key function is used to construct a valid ALTER statement for dropping index.
     * For example, ALTER TABLE t1 DROP TABLE _index
     */

    private class IndexKeyFunc implements KeyFunc {
        public static final String KEY = "_index";

        @Override
        public void generateAST(ASTNode parent) {
            MySQLIndex index;
            try {
                index = ((MySQLSchema) globalState.getSchema()).getRandomIndex();
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There are no available indexes for _index.");
            }
            String tableName = index.getTableName();
            // fix syntax error
            ASTNode oldChild = parent.getParent().getChildByName("_table");
            parent.getParent().replaceChild(oldChild, new ASTNode(new Token(Token.TokenType.TERMINAL, tableName, true)));
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, index.getName())));
        }
    }

    /**
     * This key function is used to fetch a distinct column from current column for primary key or unique.
     * For example, CREATE TABLE _table (first_new_column, PRIMARY KEY (_distinct_key_column))
     */
    private class DistinctKeyKeyFunc implements KeyFunc {

        public static final String KEY = "_distinct_key_column";

        @Override
        public void generateAST(ASTNode parent) {
            List<AbstractTableColumn<?, ?>> columns = currentContext.getCurrentColumns();
            List<AbstractTableColumn<?, ?>> distinctCols = columns.stream()
                    .filter(element -> !currentContext.getSelectedColumns().contains(element) && !element.isGenerated())
                    .collect(Collectors.toList());
            if (currentContext.getCurrentColumns().isEmpty()) {
                currentContext.addFiller(new ColumnReferenceFiller(parent, this));
            }
            if (parent.getParent().toQueryString().endsWith("PRIMARY KEY (")
                    || (parent.getParent().toQueryString().endsWith(",")
                    && parent.getParent().getParent().toQueryString().contains("PRIMARY KEY ("))) {
                distinctCols = distinctCols.stream()
                        .filter(element -> !isNull(parent, element))
                        .collect(Collectors.toList());
            } // nullable columns cannot be used in primary key
            if (!distinctCols.isEmpty()) {
                AbstractTableColumn<?, ?> col = Randomly.fromList(distinctCols);
                currentContext.addSelectedColumn(col);
                boolean needLength = isTextOrBlob(parent, col);
                if (col instanceof MySQLSchema.MySQLColumn) {
                    MySQLSchema.MySQLColumn mySQLColumn = (MySQLSchema.MySQLColumn) col;
                    if (!needLength) {
                        if (mySQLColumn.getType() != null) {
                            needLength = mySQLColumn.getType().isTextOrBlob();
                        }
                    }
                }
                if (needLength) {
                    String keyLength = new IntGenerator(1, 5, "").generate(globalState);
                    ASTNode colNode = new ASTNode(new Token(Token.TokenType.TERMINAL, getColumnName(col) + "(" + keyLength + ")"));
                    parent.addChild(colNode);
                } else {
                    ASTNode colNode = new ASTNode(new Token(Token.TokenType.TERMINAL, getColumnName(col)));
                    parent.addChild(colNode);
                }
            } else {
                throw new QueryGenerationException("There is no more column for _distinct_key_column.");
            }
        }
    }

    /**
     * This key function is used to fetch an existing column for foreign key. For example, CREATE
     * TABLE _table (first_new_column, FOREIGN KEY (_fk_column) REFERENCES _reference_table)
     */
    private class ForeignKeyColumnKeyFunc implements KeyFunc {

        public static final String KEY = "_fk_column";

        @Override
        public void generateAST(ASTNode parent) {
            List<AbstractTableColumn<?, ?>> columns = new ArrayList<>(currentContext.getCurrentColumns());

            if (columns.isEmpty()) {
                currentContext.addFiller(new ColumnReferenceFiller(parent, this));
            } else {
                AbstractTableColumn<?, ?> col = Randomly.fromList(columns);
                currentContext.addSelectedColumn(col);
                if (isTextOrBlob(parent, col)) {
                    String keyLength = new IntGenerator(1, 50, "").generate(globalState);
                    ASTNode columnNode = new ASTNode(new Token(Token.TokenType.TERMINAL, getColumnName(col) + "(" + keyLength + ")"));
                    parent.addChild(columnNode);
                } else {
                    ASTNode columnNode = new ASTNode(new Token(Token.TokenType.TERMINAL, getColumnName(col)));
                    parent.addChild(columnNode);
                }
            }
        }
    }

    private boolean isTextOrBlob(ASTNode parent, AbstractTableColumn<?, ?> column) {
        ASTNode root = new ASTNode(parent);
        while (root.getParent() != null) {
            root = root.getParent();
        }
        int ind = root.toQueryString().indexOf(getColumnName(column)) + getColumnName(column).length() + 1;
        String dataType = root.toQueryString().substring(ind).split(" ")[0];
        return dataType.contains("TEXT") || dataType.contains("BLOB");
    }

    private boolean isNull(ASTNode parent, AbstractTableColumn<?, ?> column) {
        ASTNode root = new ASTNode(parent);
        while (root.getParent() != null) {
            root = root.getParent();
        }
        String str = root.toQueryString();
        int ind = str.indexOf(getColumnName(column)) + getColumnName(column).length() + 1;
        String constraint = str.substring(ind);
        ind = constraint.indexOf(",");
        if (ind != -1) {
            constraint = constraint.substring(0, ind);
        }
        return constraint.contains("NULL") && !constraint.contains("NOT NULL");
    }

    /**
     * This key function generates INSERT INTO table SELECT * FROM table statement
     * ensuring both references use the same table.
     */
    private class InsertSelectSameTableKeyFunc implements KeyFunc {

        public static final String KEY = "_insert_select_same_table";

        @Override
        public void generateAST(ASTNode parent) {
            AbstractTable<?, ?, ?> table;
            try {
                table = globalState.getSchema().getRandomTable();
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There are no available tables for _insert_select_same_table.");
            }

            currentContext.addSelectedTable(table);
            currentContext.getCurrentColumns().addAll(table.getColumns());

            // INSERT INTO table
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, "INSERT INTO ")));
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, table.getName())));

            // (columns)
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, " (")));
            List<AbstractTableColumn<?, ?>> insertableCols = table.getColumns().stream()
                    .filter(c -> !c.isGenerated())
                    .collect(Collectors.toList());
            for (int i = 0; i < insertableCols.size(); i++) {
                parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, insertableCols.get(i).getName())));
                if (i < insertableCols.size() - 1) {
                    parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, ", ")));
                }
            }
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, ") ")));

            // SELECT the same writable columns from table
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, "SELECT ")));
            for (int i = 0; i < insertableCols.size(); i++) {
                parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, insertableCols.get(i).getName())));
                if (i < insertableCols.size() - 1) {
                    parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, ", ")));
                }
            }
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, " FROM ")));
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, table.getName())));
        }
    }

    private class ExistingColumnAliasKeyFunc implements KeyFunc {

        public static final String KEY = "_existing_column_alias";

        @Override
        public void generateAST(ASTNode parent) {
            List<String> aliases = currentContext.getAvailableColumnAliases();
            if (aliases.isEmpty()) {
                throw new QueryGenerationException("There are no available aliases to reference.");
            }
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, Randomly.fromList(aliases))));
        }
    }

    private class InsertDuplicateUpdateColumnKeyFunc implements KeyFunc {

        public static final String KEY = "_insert_duplicate_update_column";

        @Override
        @SuppressWarnings("unchecked")
        public void generateAST(ASTNode parent) {
            Object rawColumns = currentContext.getProperty("insertable_columns");
            if (!(rawColumns instanceof List<?>)) {
                throw new QueryGenerationException("There are no insertable columns for duplicate-key update.");
            }
            List<AbstractTableColumn<?, ?>> writableColumns = ((List<AbstractTableColumn<?, ?>>) rawColumns).stream()
                    .filter(col -> !col.isGenerated())
                    .collect(Collectors.toList());
            if (writableColumns.isEmpty()) {
                throw new QueryGenerationException("There are no writable columns for duplicate-key update.");
            }
            AbstractTableColumn<?, ?> column = Randomly.fromList(writableColumns);
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, getColumnName(column))));
        }
    }

    private class StressInsertValueKeyFunc implements KeyFunc {

        @Override
        public void generateAST(ASTNode parent) {
            int colSize = currentContext.getReturnedColumns().size();
            for (int i = 0; i < colSize; i++) {
                AbstractTableColumn<?, ?> col = currentContext.getReturnedColumns().poll();
                String value = generateStressSafeValue(col);
                ASTNode valueNode = new ASTNode(new Token(Token.TokenType.TERMINAL, value));
                parent.addChild(valueNode);
                if (i != colSize - 1) {
                    parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, ",")));
                }
            }
        }
    }

    private class StressAssignValueKeyFunc implements KeyFunc {

        @Override
        public void generateAST(ASTNode parent) {
            int index = currentContext.getValueIndex();
            AbstractTableColumn<?, ?> column = currentContext.getSelectedColumns().get(index);
            currentContext.increaseValueIndex();
            ASTNode valueNode = new ASTNode(new Token(Token.TokenType.TERMINAL, generateStressSafeValue(column)));
            parent.addChild(valueNode);
        }
    }

    private String generateStressSafeValue(AbstractTableColumn<?, ?> column) {
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
                return Randomly.fromOptions("-1", "0", "1");
            case "smallint":
            case "int2":
                return Randomly.fromOptions("-1", "0", "1", "127");
            case "mediumint":
            case "int3":
            case "int":
            case "integer":
            case "int4":
            case "bigint":
            case "int8":
                return Randomly.fromOptions("-1", "0", "1", "42");
            case "year":
                return Randomly.fromOptions("1970", "2000", "2024");
            case "bit":
                return Randomly.fromOptions("b'0'", "b'1'");
            case "float":
            case "double":
            case "decimal":
            case "numeric":
            case "dec":
            case "fixed":
                return Randomly.fromOptions("-1", "0", "1", "3.14", "12.5");
            case "date":
                return Randomly.fromOptions("'2000-01-01'", "'2024-12-31'");
            case "time":
                return Randomly.fromOptions("'00:00:01'", "'12:00:00'", "'23:59:59'");
            case "datetime":
            case "timestamp":
                return Randomly.fromOptions("'2000-01-01 00:00:01'", "'2024-12-31 23:59:59'");
            case "json":
                return Randomly.fromOptions("'{}'", "'[]'", "'{\"k\":\"v\"}'", "'123'");
            case "binary":
            case "varbinary":
            case "blob":
            case "tinyblob":
            case "mediumblob":
            case "longblob":
                return Randomly.fromOptions("X'00'", "X'01'", "X'4142'");
            case "char":
            case "varchar":
            case "text":
            case "tinytext":
            case "mediumtext":
            case "longtext":
            case "enum":
            case "set":
            default:
                return Randomly.fromOptions("'a'", "'b'", "'test'", "''");
        }
    }

}
