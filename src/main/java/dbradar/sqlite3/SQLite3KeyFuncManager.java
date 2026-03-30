package dbradar.sqlite3;

import dbradar.IgnoreMeException;
import grammar.Token;
import grammar.Token.TokenType;
import dbradar.common.query.generator.*;
import dbradar.sqlite3.schema.SQLite3Index;
import dbradar.sqlite3.schema.SQLite3Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SQLite3KeyFuncManager extends KeyFuncManager {

    public SQLite3KeyFuncManager(SQLite3GlobalState globalState) {
        super(globalState);

        keyFuncMap.put(NewVirtualTableKeyFunc.KEY, new NewVirtualTableKeyFunc());

        keyFuncMap.put(IndexKeyFunc.KEY, new IndexKeyFunc());

        keyFuncMap.put(TempTableKeyFunc.KEY, new TempTableKeyFunc());

        keyFuncMap.put(SQLiteStatTableKeyFunc.KEY, new SQLiteStatTableKeyFunc());

        keyFuncMap.put(VirtualTableKeyFunc.KEY, new VirtualTableKeyFunc());
        keyFuncMap.put(VirtualTableContextKeyFunc.KEY, new VirtualTableContextKeyFunc());
    }

    private class NewVirtualTableKeyFunc implements KeyFunc {

        public static final String KEY = "_new_virtual_table";

        @Override
        public void generateAST(ASTNode parent) {
            String virtualTableName = "v" + globalState.getSchema().getFreeTableName();
            parent.addChild(new ASTNode(new Token(TokenType.TERMINAL, virtualTableName)));
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
            SQLite3GlobalState sqLite3GlobalState = (SQLite3GlobalState) globalState;
            String indexName;
            try {
                SQLite3Index index = sqLite3GlobalState.getSchema().getRandomIndex(i -> !i.isUnique());
                indexName = index.getName();
                if (index.isTemporary()) {
                    indexName = "temp." + indexName; // temporary index
                }
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There are no available indexes for _index.");
            }
            parent.addChild(new ASTNode(new Token(TokenType.TERMINAL, indexName)));
        }

    }

    /**
     * This key function is used to fetch an existing table that is temp. For
     * example, PRAGMA temp.quick_check(_table_temp)
     */
    private class TempTableKeyFunc implements KeyFunc {

        public static final String KEY = "_table_temp";

        @Override
        public void generateAST(ASTNode parent) {
            SQLite3GlobalState sqLite3GlobalState = (SQLite3GlobalState) globalState;

            SQLite3Table table;
            try {
                table = sqLite3GlobalState.getSchema().getRandomTable(SQLite3Table::isTemporary);
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There are not available tables for _table_temp.");
            }

            ASTNode tableNode = new ASTNode(new Token(TokenType.TERMINAL, table.getName()));
            parent.addChild(tableNode);
        }
    }

    private class SQLiteStatTableKeyFunc implements KeyFunc {

        public static final String KEY = "_sqlite_stat1";

        @Override
        public void generateAST(ASTNode parent) {
            List<AliasTableColumn<?>> columns = new ArrayList<>();
            AliasTableColumn<?> tbl = new AliasTableColumn<>("tbl", null, null);
            columns.add(tbl);
            AliasTableColumn<?> idx = new AliasTableColumn<>("idx", null, null);
            columns.add(idx);
            AliasTableColumn<?> stat = new AliasTableColumn<>("stat", null, null);
            columns.add(stat);

            AliasTable<?, ?, ?> table = new AliasTable<>("sqlite_stat1", columns, Collections.emptyList());

            currentContext.addSelectedTable(table);
            currentContext.getCurrentColumns().addAll(table.getColumns());

            ASTNode tableNode = new ASTNode(new Token(TokenType.TERMINAL, table.getName()));
            parent.addChild(tableNode);
        }
    }

    /**
     * This key function is used to fetch an existing virtual table. For example,
     * INSERT INTO _virtual_table (_selected_virtual_table, rank) VALUES
     * ('automerge', _int4_unsigned)
     */
    private class VirtualTableKeyFunc implements KeyFunc {

        public static final String KEY = "_virtual_table";

        @Override
        public void generateAST(ASTNode parent) {
            SQLite3GlobalState sqLite3GlobalState = (SQLite3GlobalState) globalState;

            SQLite3Table vTable;
            try {
                vTable = sqLite3GlobalState.getSchema().getRandomTable(SQLite3Table::isVirtual);
            } catch (IgnoreMeException ignored) {
                throw new QueryGenerationException("There are not available virtual tables for _virtual_table.");
            }

            ASTNode tableNode = new ASTNode(new Token(TokenType.TERMINAL, vTable.getName()));
            parent.addChild(tableNode);
            currentContext.setProperty(KEY, vTable);
        }
    }

    /**
     * This key function is used to ensure that the virtual table is the same as the
     * previous virtual table For example, INSERT INTO _virtual_table (
     * _selected_virtual_table, rank) VALUES ('automerge', _int4_unsigned) if
     * _selected_virtual_table is different from _virtual_table, there will be a
     * syntax error
     */
    private class VirtualTableContextKeyFunc implements KeyFunc {

        public static final String KEY = "_selected_virtual_table";

        @Override
        public void generateAST(ASTNode parent) {
            SQLite3Table vTable = (SQLite3Table) currentContext.getProperty(VirtualTableKeyFunc.KEY);
            if (vTable == null) {
                throw new QueryGenerationException("Current selected table is empty");
            }
            ASTNode tableNode = new ASTNode(new Token(TokenType.TERMINAL, vTable.getName()));
            parent.addChild(tableNode);
        }
    }
}
