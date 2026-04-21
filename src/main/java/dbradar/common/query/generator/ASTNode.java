package dbradar.common.query.generator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import grammar.Token;
import grammar.Token.TokenType;

public class ASTNode implements Cloneable {

    private Token token;
    private ASTNode parent;
    private List<ASTNode> children = new ArrayList<>();

    private int reduceStep = 0;

    private int maxReduceStep;

    public ASTNode(Token token) {
        this.token = token;
    }

    public ASTNode(ASTNode node) {
        this.token = node.token;
        this.parent = node.parent;
        this.children = new ArrayList<>(node.children);
        this.maxReduceStep = getMaxReduceStep();
    }

    public Token getToken() {
        return token;
    }

    public void setToken(Token token) {
        this.token = token;
    }

    public ASTNode getParent() {
        return parent;
    }

    public void setParent(ASTNode parent) {
        this.parent = parent;
    }

    public List<ASTNode> getChildren() {
        return children;
    }

    public ASTNode getChildByName(String childName) {
        for (ASTNode child : children) {
            if (child.toString().equals(childName)) {
                return child;
            }
        }

        return null;
    }

    public List<ASTNode> getChildrenByName(String childName) {
        List<ASTNode> nodes = new ArrayList<>();
        for (ASTNode child : children) {
            if (child.toString().equals(childName)) {
                nodes.add(child);
            }
        }

        return nodes;
    }

    /**
     * 将对应名称的node更新为新的child
     */
    public void setChildrenByName(String childName, ASTNode child) {
        for (int i = 0; i < this.children.size(); i++) {
            if (this.children.get(i).getToken().getValue().contains(childName)) {
                this.children.set(i, child);
                break;
            }
        }
    }

    public void addChild(ASTNode childNode) {
        childNode.setParent(this);
        children.add(childNode);
    }

    public void replaceChild(ASTNode oldChild, ASTNode newChild) {
        newChild.setParent(this);
        children.set(children.indexOf(oldChild), newChild);
    }

    // add a child node at position index of children
    public void addChild(int index, ASTNode childNode) {
        childNode.setParent(this);
        children.add(index, childNode);
    }

    public void removeChild(ASTNode childNode) {
        children.remove(childNode);
    }

    public String toQueryString() {
        StringBuilder queryString = new StringBuilder();
        visit(this, queryString, false);
        return queryString.toString();
    }

    private void visit(ASTNode node, StringBuilder queryString, boolean hasPrespace) {
        if (node == null) {
            return;
        }
        if (node.getToken().hasType(TokenType.TERMINAL)) {
            String value = node.getToken().getValue();
            if (value == null || value.trim().isEmpty()) {
                return;
            }
            boolean preSpace = true;
            if (queryString.length() == 0) {
                preSpace = false;
            } else if (queryString.charAt(queryString.length() - 1) == '(') {
                preSpace = false;
            } else if (Arrays.asList(",", ";", ")").contains(value)) {
                preSpace = false;
            } else if (!hasPrespace) {
                // If the token does not have pre space, we should not add, either.
                preSpace = false;
            }
            if (preSpace) {
                queryString.append(" ");
            }
            queryString.append(value);
        } else {
            for (int i = 0; i < node.getChildren().size(); i++) {
                ASTNode childNode = node.getChildren().get(i);
                if (i == 0) {
                    visit(childNode, queryString, hasPrespace || childNode.getToken().hasPreSpace());
                } else {
                    visit(childNode, queryString, childNode.getToken().hasPreSpace());
                }
            }
        }
    }

    public void print() {
        internalPrint(this, "", false);
    }

    private void internalPrint(ASTNode node, String prefix, boolean isTail) {
        System.out.println(prefix + (isTail ? "└── " : "├── ") + node.getToken().getValue());

        for (int i = 0; i < node.getChildren().size() - 1; i++) {
            internalPrint(node.getChildren().get(i), prefix + (isTail ? "    " : "│   "), false);
        }

        if (!node.getChildren().isEmpty()) {
            internalPrint(node.getChildren().get(node.getChildren().size() - 1), prefix + (isTail ? "    " : "│   "),
                    true);
        }
    }

    @Override
    public String toString() {
        return token.getValue();
    }

    @Override
    public ASTNode clone() {
        try {
            ASTNode node = (ASTNode) super.clone();
            node.token = token.clone();
            node.parent = parent;
            node.children = new ArrayList<>();
            for (ASTNode child : children) {
                node.children.add(child.clone());
            }
            return node;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    public int getReduceStep() {
        return reduceStep;
    }

    public void setReduceStep(int reduceStep) {
        this.reduceStep = reduceStep;
    }

    public void incrementReduceStep() {
        this.reduceStep++;
    }

    private int getMaxReduceStep() {
        for (ASTNode child : children) {
            if (child.token.hasType(TokenType.NON_TERMINAL)) {
                maxReduceStep++;
            }
        }
        return maxReduceStep;
    }

    public int getExprCount() {
        int count = 0;
        for (ASTNode child : children) {
            if (child.token.hasType(TokenType.NON_TERMINAL)) {
                count++;
            }
        }
        return count;
    }
}
