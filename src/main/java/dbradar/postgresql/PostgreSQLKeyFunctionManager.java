package dbradar.postgresql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import dbradar.IgnoreMeException;
import dbradar.Randomly;
import dbradar.SQLGlobalState;
import grammar.Token;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.query.generator.KeyFunc;
import dbradar.common.query.generator.KeyFuncManager;
import dbradar.common.query.generator.QueryGenerationException;
import dbradar.common.query.generator.data.Generator;
import dbradar.common.query.generator.data.GeneratorRegister;
import dbradar.common.query.generator.data.IntGenerator;
import dbradar.common.query.generator.data.TextGenerator;
import dbradar.common.schema.AbstractTable;
import dbradar.common.schema.AbstractTableColumn;
import dbradar.common.schema.TableIndex;
import dbradar.postgresql.PostgreSQLSchema.PostgreSQLTable;

public class PostgreSQLKeyFunctionManager extends KeyFuncManager {
    private static final String SELECTED_PARTITION_PARENT = "selected_partition_parent";

    public PostgreSQLKeyFunctionManager(SQLGlobalState globalState) {
        super(globalState);

        keyFuncMap.put(DatabaseKeyFunc.KEY, new DatabaseKeyFunc());
        keyFuncMap.put(AccessMethodKeyFunc.KEY, new AccessMethodKeyFunc());
        keyFuncMap.put(DistinctTableKeyFunc.KEY, new DistinctTableKeyFunc());
        keyFuncMap.put(IndexKeyFunc.KEY, new IndexKeyFunc());
        keyFuncMap.put(NewConstraintNameKeyFunc.KEY, new NewConstraintNameKeyFunc());
        keyFuncMap.put(NotPKColumnKeyFunc.KEY, new NotPKColumnKeyFunc());
        keyFuncMap.put(PartitionedTableWithoutDefaultKeyFunc.KEY, new PartitionedTableWithoutDefaultKeyFunc());
        keyFuncMap.put(PartitionedTableForNewPartitionKeyFunc.KEY, new PartitionedTableForNewPartitionKeyFunc());
        keyFuncMap.put(PartitionedTableWithPartitionsKeyFunc.KEY, new PartitionedTableWithPartitionsKeyFunc());
        keyFuncMap.put(PartitionOfSelectedTableKeyFunc.KEY, new PartitionOfSelectedTableKeyFunc());
        keyFuncMap.put(DetachedPartitionCandidateKeyFunc.KEY, new DetachedPartitionCandidateKeyFunc());
        keyFuncMap.put(NewPartitionBoundKeyFunc.KEY, new NewPartitionBoundKeyFunc());
        keyFuncMap.put(InsertTargetTableKeyFunc.KEY, new InsertTargetTableKeyFunc());
        keyFuncMap.put(UpdatableTableKeyFunc.KEY, new UpdatableTableKeyFunc());
        keyFuncMap.put(PartitionAwareInsertValueKeyFunc.KEY, new PartitionAwareInsertValueKeyFunc());
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
            String prefix = globalState.getGeneratedObjectNamePrefix();
            if (!prefix.isEmpty()) {
                constraintName = prefix + constraintName;
            }
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

    private class PartitionedTableWithoutDefaultKeyFunc implements KeyFunc {
        public static final String KEY = "_partitioned_table_without_default";

        @Override
        public void generateAST(ASTNode parent) {
            PostgreSQLSchema schema = (PostgreSQLSchema) globalState.getSchema();
            PostgreSQLTable table;
            try {
                table = schema.getRandomPartitionedTableWithoutDefaultPartition();
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There is no available partitioned table without default partition.");
            }
            rememberSelectedPartitionedTable(table);
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, table.getName())));
        }
    }

    private class PartitionedTableWithPartitionsKeyFunc implements KeyFunc {
        public static final String KEY = "_partitioned_table_with_partitions";

        @Override
        public void generateAST(ASTNode parent) {
            PostgreSQLSchema schema = (PostgreSQLSchema) globalState.getSchema();
            PostgreSQLTable table;
            try {
                table = schema.getRandomPartitionedTableWithPartitions();
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There is no partitioned table with partitions.");
            }
            rememberSelectedPartitionedTable(table);
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, table.getName())));
        }
    }

    private class PartitionedTableForNewPartitionKeyFunc implements KeyFunc {
        public static final String KEY = "_partitioned_table_for_new_partition";

        @Override
        public void generateAST(ASTNode parent) {
            PostgreSQLSchema schema = (PostgreSQLSchema) globalState.getSchema();
            PostgreSQLTable table;
            try {
                table = schema.getRandomPartitionedTableForPartitionCreation();
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There is no partitioned table that can accept an additional partition.");
            }
            rememberSelectedPartitionedTable(table);
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, table.getName())));
        }
    }

    private class PartitionOfSelectedTableKeyFunc implements KeyFunc {
        public static final String KEY = "_partition_of_selected_table";

        @Override
        public void generateAST(ASTNode parent) {
            PostgreSQLSchema schema = (PostgreSQLSchema) globalState.getSchema();
            PostgreSQLTable selectedParent = getSelectedPartitionedTable();
            PostgreSQLTable partition;
            try {
                partition = schema.getRandomPartitionOf(selectedParent);
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There is no partition for the selected partitioned table.");
            }
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, partition.getName())));
        }
    }

    private class DetachedPartitionCandidateKeyFunc implements KeyFunc {
        public static final String KEY = "_detached_partition_candidate";

        @Override
        public void generateAST(ASTNode parent) {
            PostgreSQLSchema schema = (PostgreSQLSchema) globalState.getSchema();
            PostgreSQLTable selectedParent = getSelectedPartitionedTable();
            PostgreSQLTable candidate;
            try {
                candidate = schema.getRandomDetachedPartitionCandidate(selectedParent);
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There is no detached partition candidate for the selected partitioned table.");
            }
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, candidate.getName())));
        }
    }

    private class NewPartitionBoundKeyFunc implements KeyFunc {
        public static final String KEY = "_new_partition_bound";

        @Override
        public void generateAST(ASTNode parent) {
            PostgreSQLSchema schema = (PostgreSQLSchema) globalState.getSchema();
            PostgreSQLTable selectedParent = getSelectedPartitionedTable();
            String partitionBound;
            try {
                partitionBound = schema.generateNewPartitionBound(selectedParent);
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("Unable to generate a valid partition bound.");
            }
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, partitionBound)));
        }
    }

    private class InsertTargetTableKeyFunc implements KeyFunc {
        public static final String KEY = "_insert_target_table";

        @Override
        public void generateAST(ASTNode parent) {
            PostgreSQLSchema schema = (PostgreSQLSchema) globalState.getSchema();
            PostgreSQLTable table;
            try {
                table = schema.getRandomInsertTargetTable();
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There is no available insert target table.");
            }
            rememberSelectedTable(table);
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, table.getName())));
        }
    }

    private class UpdatableTableKeyFunc implements KeyFunc {
        public static final String KEY = "_updatable_table";

        @Override
        public void generateAST(ASTNode parent) {
            PostgreSQLSchema schema = (PostgreSQLSchema) globalState.getSchema();
            PostgreSQLTable table;
            try {
                table = schema.getRandomUpdatableTable();
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There is no available updatable table.");
            }
            rememberSelectedTable(table);
            parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, table.getName())));
        }
    }

    private class PartitionAwareInsertValueKeyFunc implements KeyFunc {
        public static final String KEY = "_insert_values";

        @Override
        public void generateAST(ASTNode parent) {
            Map<String, String> partitionValues = Map.of();
            PostgreSQLTable selectedTable = getSelectedTableOrNull();
            if (selectedTable != null && selectedTable.isPartitionedTable()) {
                try {
                    partitionValues = ((PostgreSQLSchema) globalState.getSchema()).generatePartitionInsertValues(selectedTable);
                } catch (IgnoreMeException ignored) {
                    throw new QueryGenerationException("Unable to generate insert values for a partitioned table.");
                }
            }

            int colSize = currentContext.getReturnedColumns().size();
            for (int i = 0; i < colSize; i++) {
                AbstractTableColumn<?, ?> col = currentContext.getReturnedColumns().poll();
                String value = partitionValues.get(col.getName());
                if (value == null) {
                    Generator generator = GeneratorRegister.getGenerator(col, globalState);
                    value = generator.generate(globalState);
                    while (col.isNotNull() && value.equals("null")) {
                        value = generator.generate(globalState);
                    }
                }
                ASTNode valueNode = new ASTNode(new Token(Token.TokenType.TERMINAL, value));
                parent.addChild(valueNode);
                if (i != colSize - 1) {
                    parent.addChild(new ASTNode(new Token(Token.TokenType.TERMINAL, ",")));
                }
            }
        }
    }

    private PostgreSQLTable getSelectedPartitionedTable() {
        Object selectedParent = currentContext.getProperty(SELECTED_PARTITION_PARENT);
        if (!(selectedParent instanceof PostgreSQLTable)) {
            throw new QueryGenerationException("No selected partitioned table.");
        }
        return (PostgreSQLTable) selectedParent;
    }

    private void rememberSelectedPartitionedTable(PostgreSQLTable table) {
        rememberSelectedTable(table);
        currentContext.setProperty(SELECTED_PARTITION_PARENT, table);
    }

    private void rememberSelectedTable(PostgreSQLTable table) {
        currentContext.addSelectedTable(table);
        currentContext.getCurrentColumns().clear();
        currentContext.getCurrentColumns().addAll(table.getColumns());
    }

    private PostgreSQLTable getSelectedTableOrNull() {
        if (currentContext.getSelectedTables().isEmpty()) {
            return null;
        }
        AbstractTable<?, ?, ?> table = currentContext.getSelectedTables().get(0);
        if (table instanceof PostgreSQLTable) {
            return (PostgreSQLTable) table;
        }
        return null;
    }

}
