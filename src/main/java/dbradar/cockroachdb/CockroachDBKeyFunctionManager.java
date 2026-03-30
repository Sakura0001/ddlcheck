package dbradar.cockroachdb;

import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.SQLGlobalState;
import grammar.Token;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.query.generator.ColumnReferenceFiller;
import dbradar.common.query.generator.KeyFunc;
import dbradar.common.query.generator.KeyFuncManager;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.common.query.generator.data.IntGenerator;
import dbradar.common.query.generator.data.TextGenerator;
import dbradar.common.schema.AbstractTableColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import dbradar.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import dbradar.cockroachdb.CockroachDBSchema.CockroachDBTable;
import dbradar.cockroachdb.CockroachDBSchema.CockroachDBIndex;

public class CockroachDBKeyFunctionManager extends KeyFuncManager {
    public CockroachDBKeyFunctionManager(SQLGlobalState globalState) {
        super(globalState);

        keyFuncMap.put(DistinctTableKeyFunc.KEY, new DistinctTableKeyFunc());
        keyFuncMap.put(IndexKeyFunc.KEY, new IndexKeyFunc());
        keyFuncMap.put(NewConstraintNameKeyFunc.KEY, new NewConstraintNameKeyFunc());
        keyFuncMap.put(ColumnKeyFunc.KEY, new ColumnKeyFunc());
        keyFuncMap.put(NotPKColumnKeyFunc.KEY, new NotPKColumnKeyFunc());
        keyFuncMap.put(DistinctColumnKeyFunc.KEY, new DistinctColumnKeyFunc());
        keyFuncMap.put(AllColumnKeyFunc.KEY, new AllColumnKeyFunc());
        keyFuncMap.put(NewViewNameKeyFunc.KEY, new NewViewNameKeyFunc());
        keyFuncMap.put(ViewKeyFunc.KEY, new ViewKeyFunc());
    }

    /**
     * This key function is used to fetch a distinct table.
     * For example, TRUNCATE TABLE _distinct_table , _distinct_table
     */
    private class DistinctTableKeyFunc implements KeyFunc {

        public static final String KEY = "_distinct_table";

        @Override
        public void generateAST(ASTNode parent) {
            CockroachDBGlobalState state = (CockroachDBGlobalState) globalState;

            try {
                List<CockroachDBTable> tables = state.getSchema().getDatabaseTablesWithoutViews();
                List<CockroachDBTable> distinctTables = tables.stream()
                        .filter(element -> !currentContext.getSelectedTables().contains(element))
                        .collect(Collectors.toList());
                if (!distinctTables.isEmpty()) {
                    CockroachDBTable table = Randomly.fromList(distinctTables);
                    currentContext.addSelectedTable(table);
                    currentContext.getCurrentColumns().addAll(table.getColumns());
                    ASTNode tableNode = new ASTNode(new Token(Token.TokenType.TERMINAL, table.getName()));
                    parent.addChild(tableNode);
                } else {
                    throw new QueryGenerationException("There are not available tables for _distinct_table.");
                }
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There are not available tables for _distinct_table.");
            }
        }
    }

    /**
     * This key function is used to return an existing index. For example, DROP
     * INDEX _index
     */
    private class IndexKeyFunc implements KeyFunc {
        public static final String KEY = "_index";

        @Override
        public void generateAST(ASTNode parent) {
            CockroachDBGlobalState state = (CockroachDBGlobalState) globalState;
            String indexName;
            try {
                CockroachDBIndex index = state.getSchema().getRandomIndex();
                indexName = index.getName();
                for (int i = 0; i < 10; i++) {
                    if (indexName.contains("_pkey") || indexName.contains("_key")) { // do not drop automatically built indexes
                        index = state.getSchema().getRandomIndex();
                        indexName = index.getName();
                    } else {
                        break;
                    }
                }
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There are no available indexes for _index.");
            }
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, indexName)));
        }
    }

    /**
     * This key function is used to fetch a new constraint name. For example, ALTER
     * TABLE _TABLE ADD CONSTRAINT _new_constraint_name UNIQUE _index
     */
    private class NewConstraintNameKeyFunc implements KeyFunc {
        public static final String KEY = "_new_constraint_name";

        @Override
        public void generateAST(ASTNode parent) {
            int length = Integer.parseInt(new IntGenerator(1, 10, "").generate(globalState));
            String constraintName = new TextGenerator(length).generate(globalState);
            constraintName = constraintName.substring(1, constraintName.length() - 1);
            ASTNode tableNode = new ASTNode(new Token(Token.TokenType.TERMINAL, constraintName));
            parent.addChild(tableNode);
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

                List<AbstractTableColumn<?, ?>> columnsWithoutRowid = new ArrayList<>();

                for (AbstractTableColumn<?, ?> col : columns) {
                    if (!col.getName().contains("rowid")) { // may have multiple columns or alias columns
                        columnsWithoutRowid.add(col);
                    }
                }

                if (columnsWithoutRowid.isEmpty()) {
                    throw new QueryGenerationException("There is not suitable column for _column");
                }

                AbstractTableColumn<?, ?> col = Randomly.fromList(columnsWithoutRowid);
                currentContext.addSelectedColumn(col);
                ASTNode columnNode = new ASTNode(new Token(Token.TokenType.TERMINAL, getColumnName(col)));
                parent.addChild(columnNode);
            }
        }
    }

    /**
     * This key function is used to return an existing column which is not primary key. For example,
     * ALTER TABLE _table ALTER _not_pk_column DROP NOT NULL
     */
    private class NotPKColumnKeyFunc implements KeyFunc {
        public static final String KEY = "_not_pk_column";

        @Override
        public void generateAST(ASTNode parent) {
            List<AbstractTableColumn<?, ?>> columns = new ArrayList<>();
            for (AbstractTableColumn<?, ?> col : currentContext.getCurrentColumns()) {
                if (!col.isPrimaryKey()) {
                    columns.add(col);
                }
            }
            AbstractTableColumn<?, ?> col = Randomly.fromList(columns);
            currentContext.addSelectedColumn(col);
            ASTNode columnNode = new ASTNode(new Token(Token.TokenType.TERMINAL, getColumnName(col)));
            parent.addChild(columnNode);
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
                    .filter(element -> !currentContext.getSelectedColumns().contains(element) && !element.isGenerated() && !element.getName().equals("rowid"))
                    .collect(Collectors.toList());
            if (currentContext.getCurrentColumns().isEmpty()) {
                currentContext.addFiller(new ColumnReferenceFiller(parent, this));
            }
            if (!distinctCols.isEmpty()) {
                AbstractTableColumn<?, ?> col = Randomly.fromList(distinctCols);
                currentContext.addSelectedColumn(col);
                ASTNode colNode = new ASTNode(new Token(Token.TokenType.TERMINAL, getColumnName(col)));
                parent.addChild(colNode);
            } else {
                throw new QueryGenerationException("There is no more column for _distinct_column.");
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
                List<AbstractTableColumn<?, ?>> candidateCols = columns.stream().filter(c -> !c.isGenerated() && !c.getName().equals("rowid"))
                        .collect(Collectors.toList());
                for (int i = 0; i < candidateCols.size(); i++) {
                    AbstractTableColumn<?, ?> col = candidateCols.get(i);
                    currentContext.addReturnedColumn(col);
                    ASTNode colNode = new ASTNode(new Token(Token.TokenType.TERMINAL, getColumnName(col)));
                    parent.addChild(colNode);
                    if (i != candidateCols.size() - 1) {
                        parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, ",")));
                    }
                }
            }
        }
    }

    /**
     * This key function is used to return a new view name For example, CREATE VIEW
     * _new_view_name (_new_column) AS select
     * When the generated view is materialized, we generate another name start with vt
     */
    private class NewViewNameKeyFunc implements KeyFunc {
        public static final String KEY = "_new_view_name";

        @Override
        public void generateAST(ASTNode parent) {
            String viewName = globalState.getSchema().getFreeViewName();
            if (parent.getParent().getChildByName("MATERIALIZED") != null) {
                viewName = viewName.replaceFirst("v", "vt");
            }
            ASTNode tableNode = new ASTNode(new Token(Token.TokenType.TERMINAL, viewName));
            parent.addChild(tableNode);
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
            CockroachDBGlobalState state = (CockroachDBGlobalState) globalState;
            String viewName;
            try {
                boolean dropMaterializedView = parent.getParent().getChildByName("MATERIALIZED") != null;
                if (dropMaterializedView) {
                    viewName = state.getSchema().getRandomMaterializedView().getName();
                } else {
                    viewName = state.getSchema().getRandomNormalView().getName();
                }
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There are no available views for _view.");
            }

            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, viewName)));
        }

    }
}
