package dbradar.common.query.generator;

public interface KeyFunc {

    /**
     * Analyze the key function, and append the generated AST nodes as parentNode's
     * children.
     * 
     * @param parent The parent node of the analyzed key function.
     */
    void generateAST(ASTNode parent);

}
