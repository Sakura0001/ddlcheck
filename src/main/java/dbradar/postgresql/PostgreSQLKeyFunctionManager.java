package dbradar.postgresql;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.SQLGlobalState;
import grammar.Token;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.query.generator.KeyFunc;
import dbradar.common.query.generator.KeyFuncManager;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.common.query.generator.data.IntGenerator;
import dbradar.common.query.generator.data.TextGenerator;
import dbradar.common.schema.AbstractTable;
import dbradar.common.schema.AbstractTableColumn;
import dbradar.common.schema.TableIndex;

public class PostgreSQLKeyFunctionManager extends KeyFuncManager {
    public PostgreSQLKeyFunctionManager(SQLGlobalState globalState) {
        super(globalState);

        keyFuncMap.put(DatabaseKeyFunc.KEY, new DatabaseKeyFunc());
        keyFuncMap.put(AccessMethodKeyFunc.KEY, new AccessMethodKeyFunc());
        keyFuncMap.put(DistinctTableKeyFunc.KEY, new DistinctTableKeyFunc());
        keyFuncMap.put(IndexKeyFunc.KEY, new IndexKeyFunc());
        keyFuncMap.put(NewConstraintNameKeyFunc.KEY, new NewConstraintNameKeyFunc());
        keyFuncMap.put(NotPKColumnKeyFunc.KEY, new NotPKColumnKeyFunc());
    }


    /**
     * This key function is used to return the database name. For example,
     * REINDEX DATABASE _database
     */
    private class DatabaseKeyFunc implements KeyFunc {

        public static final String KEY = "_database";

        @Override
        public void generateAST(ASTNode parent) {
            String databaseName = globalState.getDatabaseName();
            ASTNode node = new ASTNode(new Token(Token.TokenType.TERMINAL, databaseName));
            parent.addChild(node);
        }
    }

    /**
     * This key function is used to return an access method. For example,
     * CREATE TABLE t1 (c1 INT) USING _access_method
     */
    private class AccessMethodKeyFunc implements KeyFunc {

        public static final String KEY = "_access_method";

        @Override
        public void generateAST(ASTNode parent) {
            PostgreSQLGlobalState state = (PostgreSQLGlobalState) globalState;
            String accessMethod = state.getRandomTableAccessMethod();
            ASTNode node = new ASTNode(new Token(Token.TokenType.TERMINAL, accessMethod));
            parent.addChild(node);
        }
    }

    /**
     * This key function is used to fetch a distinct table.
     * For example, TRUNCATE TABLE _distinct_table , _distinct_table
     */
    private class DistinctTableKeyFunc implements KeyFunc {

        public static final String KEY = "_distinct_table";

        @Override
        public void generateAST(ASTNode parent) {
            try {
                List<AbstractTable<?, ?, ?>> tables = (List<AbstractTable<?, ?, ?>>) globalState.getSchema().getDatabaseTablesWithoutViews();
                List<AbstractTable<?, ?, ?>> distinctTables = tables.stream()
                        .filter(element -> !currentContext.getSelectedTables().contains(element))
                        .collect(Collectors.toList());
                if (!distinctTables.isEmpty()) {
                    AbstractTable<?, ?, ?> table = Randomly.fromList(distinctTables);
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
            String indexName;
            try {
                TableIndex index = ((PostgreSQLSchema) globalState.getSchema()).getRandomIndex();
                indexName = index.getName();
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
            if (columns.isEmpty()) {
                throw new QueryGenerationException("No such column");
            }
            AbstractTableColumn<?, ?> col = Randomly.fromList(columns);
            currentContext.addSelectedColumn(col);
            ASTNode columnNode = new ASTNode(new Token(Token.TokenType.TERMINAL, getColumnName(col)));
            parent.addChild(columnNode);
        }
    }

}
