package dbradar.common.duplicate;

import dbradar.GlobalState;
import dbradar.Reproducer;
import grammar.Token;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.ASTNode;
import dbradar.common.schema.AbstractTable;
import dbradar.common.schema.AbstractTableColumn;
import dbradar.common.schema.TableIndex;

import java.io.*;
import java.util.*;

public class Feature {
    private HashSet<String> column;
    private HashSet<String> constraint;
    private HashSet<String> operator;
    private HashSet<String> function;

    public Feature() {
    }

    public Feature(HashSet<String> column, HashSet<String> constraint, HashSet<String> function, HashSet<String> operator) {
        this.column = column;
        this.constraint = constraint;
        this.function = function;
        this.operator = operator;
    }

    /**
     * 从数据表和执行信息中获取缺陷触发相关的四维特征
     */
    public Feature recordFactor(GlobalState state, Reproducer reproducer) {
        try {
            state.updateSchema();
            recordColumn(state);
            recordConstraint(state);
            recordOperator(reproducer);
            recordFunction(state, reproducer);

            return this;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private void recordColumn(GlobalState state) {
        HashSet<String> colVec = new HashSet<>();
        for (AbstractTable<?, ?, ?> table : state.getSchema().getDatabaseTables()) {
            for (AbstractTableColumn<?, ?> column : table.getColumns()) {
                colVec.add(column.getType().toString());
            }
        }
        setColumn(colVec);
    }

    private void recordConstraint(GlobalState state) {
        HashSet<String> consVec = new HashSet<>();
        for (AbstractTable<?, ?, ?> table : state.getSchema().getDatabaseTables()) {
            for (TableIndex index : table.getIndexes()) {
                //index可能需要加一个type
                consVec.add(index.toString());
            }
        }
        setConstraint(consVec);
    }

    private void recordFunction(GlobalState state, Reproducer reproducer) {
        HashSet<String> funcVec;
        HashSet<String> deleteFunc;
        try {
            //执行测试预言语句并记录底层函数调用
            SQLQueryAdapter oracle = reproducer.getOracleStatements().get(0);
            oracle.execute(state);
            funcVec = readOracleFunc();

            //执行标准全表查询语句并记录底层函数调用
            oracle.getQueryAST().setChildrenByName("where_clause", new ASTNode(new Token(Token.TokenType.TERMINAL, "TRUE")));
            deleteFunc = readOriginFunc();

            for (String func : deleteFunc) {
                funcVec.remove(func);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        setFunction(funcVec);
    }

    private HashSet<String> readOriginFunc() {
        HashSet<String> funcVec = new HashSet<>();
        try {
            FileReader fr = new FileReader("report/func/origin.txt");
            BufferedReader br = new BufferedReader(fr);
            while (br.ready()) {
                funcVec.add(br.readLine());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return funcVec;
    }

    private HashSet<String> readOracleFunc() {
        HashSet<String> funcVec = new HashSet<>();
        try {
            FileReader fr = new FileReader("report/func/oracle.txt");
            BufferedReader br = new BufferedReader(fr);
            while (br.ready()) {
                funcVec.add(br.readLine());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return funcVec;
    }

    private void recordOperator(Reproducer reproducer) {
        //TODO: 此为norec（且第一条语句为带有where谓词的语句）的where语句所在的位置，其他测试预言类型需要做适配
        ASTNode whereClause = reproducer.getOracleStatements().get(0).getQueryAST().getChildByName("where_clause");
        //从ASTNode获取所有的运算符以及操作数
        HashSet<String> opVec = new HashSet<>();
        recursion(whereClause, opVec);
        setOperator(opVec);
    }

    private void recursion(ASTNode node, HashSet<String> opVec) {
        opVec.add(node.getToken().getValue());
        if (node.getChildren() != null) {
            for (ASTNode child : node.getChildren()) {
                recursion(child, opVec);
            }
        }
    }

    /**
     * 计算相似度
     */
    public static double calculateSimilarity(Feature feature1, Feature feature2, Weight weight) {
        double similarity = 0;
        similarity += weight.getColumnWeight() * product(feature1.getColumn(), feature2.getColumn());
        similarity += weight.getConstraintWeight() * product(feature1.getConstraint(), feature2.getConstraint());
        similarity += weight.getOperationWeight() * product(feature1.getOperator(), feature2.getOperator());
        similarity += weight.getFunctionWeight() * product(feature1.getFunction(), feature2.getFunction());
        return similarity;
    }

    public static double product(HashSet<String> vec1, HashSet<String> vec2) {
        double product = 0;
        for (String v : vec1) {
            if (!vec2.contains(v)) {
                product += 1;
            }
        }
        for (String v : vec2) {
            if (!vec1.contains(v)) {
                product += 1;
            }
        }
        return product;
    }

    public HashSet<String> getColumn() {
        return column;
    }

    public void setColumn(HashSet<String> column) {
        this.column = column;
    }

    public HashSet<String> getConstraint() {
        return constraint;
    }

    public void setConstraint(HashSet<String> constraint) {
        this.constraint = constraint;
    }

    public HashSet<String> getFunction() {
        return function;
    }

    public void setFunction(HashSet<String> function) {
        this.function = function;
    }

    public HashSet<String> getOperator() {
        return operator;
    }

    public void setOperator(HashSet<String> operator) {
        this.operator = operator;
    }
}

