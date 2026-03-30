package dbradar.common.query.generator;

public class ColumnReferenceFiller {

    private ASTNode parentNode;
    private KeyFunc func;

    private boolean filled = false;

    public ColumnReferenceFiller(ASTNode parentNode, KeyFunc func) {
        this.parentNode = parentNode;
        this.func = func;
    }

    public boolean isFilled() {
        return filled;
    }

    public void fill() {
        func.generateAST(parentNode);
    }
}
