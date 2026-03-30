package dbradar.mariadb;

import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.SQLGlobalState;
import grammar.Token;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.query.generator.AliasTable;
import dbradar.common.query.generator.AliasTableColumn;
import dbradar.common.query.generator.ColumnReferenceFiller;
import dbradar.common.query.generator.KeyFunc;
import dbradar.common.query.generator.KeyFuncManager;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.common.query.generator.data.IntGenerator;
import dbradar.common.schema.AbstractTable;
import dbradar.common.schema.AbstractTableColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MariaDBKeyFuncManager extends KeyFuncManager {
    public MariaDBKeyFuncManager(SQLGlobalState globalState) {
        super(globalState);

        keyFuncMap.put(DropPrimaryKeyKeyFunc.KEY, new DropPrimaryKeyKeyFunc());
        keyFuncMap.put(DropIndexKeyFunc.KEY, new DropIndexKeyFunc());
        keyFuncMap.put(IndexKeyFunc.KEY, new IndexKeyFunc());
        keyFuncMap.put(TableKeyFunc.KEY, new TableKeyFunc());
        keyFuncMap.put(DistinctKeyKeyFunc.KEY, new DistinctKeyKeyFunc());
        keyFuncMap.put(ForeignKeyColumnKeyFunc.KEY, new ForeignKeyColumnKeyFunc());
        keyFuncMap.put(MultipleTablesKeyFunc.KEY, new MultipleTablesKeyFunc());
    }

    /**
     * This key function is used to determine whether there are primary key to drop.
     * For example, ALTER TABLE _table _drop_primary_key
     */
    private class DropPrimaryKeyKeyFunc implements KeyFunc {

        public static final String KEY = "_drop_primary_key";

        @Override
        public void generateAST(ASTNode parent) {
            if (currentContext.getSelectedTables().get(0).hasPrimaryKey()) {
                parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, "DROP PRIMARY KEY")));
            } else {
                throw new QueryGenerationException("There are not available primary key to drop.");
            }
        }
    }

    private class DropIndexKeyFunc implements KeyFunc {
        public static final String KEY = "_drop_index";

        @Override
        public void generateAST(ASTNode parent) {
            MariaDBSchema.MariaDBIndex index;
            try {
                index = ((MariaDBSchema) globalState.getSchema()).getRandomIndex();
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
            MariaDBSchema.MariaDBIndex index;
            try {
                index = ((MariaDBSchema) globalState.getSchema()).getRandomIndex();
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
            }
            if (!distinctCols.isEmpty()) {
                AbstractTableColumn<?, ?> col = Randomly.fromList(distinctCols);
                currentContext.addSelectedColumn(col);
                if (isTextOrBlob(parent, col)) {
                    String keyLength = new IntGenerator(1, 50, "").generate(globalState);
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

    /**
     * This key function is used to fetch one or more distinct tables. For example,
     * UPDATE _distinct_table more_distinct_table* SET assignment assignment_more* where_clause?
     */
    private class MultipleTablesKeyFunc implements KeyFunc {

        public static final String KEY = "_distinct_table";

        @Override
        public void generateAST(ASTNode parent) {
            AbstractTable<?, ?, ?> table;
            try {
                table = globalState.getSchema().getRandomTable();
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There are not available tables for _distinct_table.");
            }
            while (currentContext.getSelectedTables().contains(table)) {
                try {
                    table = globalState.getSchema().getRandomTable();
                } catch (IgnoreMeException ignored) {
                    throw new QueryGenerationException("There are not available tables for _distinct_table.");
                }
            }
            currentContext.addSelectedTable(table);
            currentContext.getCurrentColumns().addAll(table.getColumns());
            ASTNode tableNode = new ASTNode(new Token(Token.TokenType.TERMINAL, table.getName()));
            parent.addChild(tableNode);
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
}
