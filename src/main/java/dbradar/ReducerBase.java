package dbradar;

import grammar.Token;
import dbradar.common.query.DBRadarResultSet;
import dbradar.common.query.ExpectedErrors;
import dbradar.common.query.SQLQueryAdapter;
import dbradar.common.query.generator.ASTNode;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 父类中定义一些公共的消减策略，如果某些数据库具有特殊的expression，则在自己的reducer中制定消减策略
 */
public class ReducerBase {
    protected final GlobalState state;
    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final StateLogger logger;
    protected final MainOptions options;
    protected final DatabaseConnection con;

    public ReducerBase(GlobalState state) {
        this.state = state;
        this.logger = state.getLogger();
        this.options = state.getOptions();
        this.con = state.getConnection();
    }

    /**
     * 如果可以直接通过select node获得节点的值，直接使用查询获取到的值
     */
    protected ASTNode queryForNodeValue(ASTNode node) {
        String selectWhere = String.format("SELECT %s", node.toQueryString());
        SQLQueryAdapter query = new SQLQueryAdapter(selectWhere);
        try (DBRadarResultSet rs = query.executeAndGet(state)) {
            if (rs == null) {
                return node;
            } else {
                rs.next();
                return new ASTNode(new Token(Token.TokenType.TERMINAL, rs.getString(1)));
            }
        } catch (SQLException ignored) {

        }
        return node;
    }

    /**
     * 统一接口，当不知道node的类型时，调用这个reduce接口，reduce接口根据对应的node类型选择合适的reduce方法
     */
    public ASTNode reduce(ASTNode node) {
        System.out.println("reducing " + node.toQueryString());
        System.out.println("reduceStep " + node.getReduceStep());
        System.out.println("==================================");
        List<ASTNode> children = node.getChildren();
        ASTNode reduceNode;
        int exprCount = node.getExprCount();
        List<Integer> positions = getExprPositions(node);

        if (exprCount == 0) {
            node.incrementReduceStep();
            reduceNode = node;
        } else {
            if (node.getReduceStep() == 0) {
                reduceNode = queryForNodeValue(node);
                node.incrementReduceStep();
            } else if (node.getReduceStep() <= exprCount * 2) {
                if (node.getReduceStep() <= exprCount) {
                    reduceNode = children.get(positions.get(node.getReduceStep() - 1));
                    node.incrementReduceStep();
                } else {
                    if (finishReduce(node)) {
                        node.incrementReduceStep();
                        redoReduceStep(children.get(node.getReduceStep() % exprCount - 1));
                    }
                    //TODO:这里会存在反复对同一个——column进行消减，而不会对后续子节点进行消减，从而产生死循环的情况，需要设置为会对后续子节点进行消减
                    children.set(positions.get(node.getReduceStep() % exprCount - 1), reduce(children.get(positions.get(node.getReduceStep() % exprCount - 1))));
                    node.incrementReduceStep();
                    reduceNode = node;
                }
            } else {
                reduceNode = node;
            }
        }
        reduceNode.getToken().setPreSpace(true);
        return reduceNode;
    }

    /**
     * Gets the location of expr in children
     */
    protected List<Integer> getExprPositions(ASTNode node) {
        List<Integer> positions = new ArrayList<>();
        for (int i = 0; i < node.getChildren().size(); i++) {
            if (node.getChildren().get(i).getToken().hasType(Token.TokenType.NON_TERMINAL)) {
                positions.add(i);
            }
        }
        return positions;
    }

    /**
     * Determines whether all reduction strategies are executed on the node
     */
    protected boolean finishReduce(ASTNode node) {
        boolean result = true;
        result &= (node.getReduceStep() == node.getExprCount() * 2 - 1 || node.getExprCount() == 0);
        for (ASTNode child : node.getChildren()) {
            result &= finishReduce(child);
        }
        return result;
    }

    /**
     * Redo the reduceStep for all child nodes:set children's reduceStep = 0
     */
    protected void redoReduceStep(ASTNode node) {
        for (ASTNode child : node.getChildren()) {
            child.setReduceStep(0);
            if (child.getChildren() != null) {
                redoReduceStep(child);
            }
        }
    }
}
