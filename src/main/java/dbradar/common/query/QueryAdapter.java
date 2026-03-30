package dbradar.common.query;

import dbradar.common.query.generator.ASTNode;

public abstract class QueryAdapter extends Query {
    protected ASTNode queryAST;

    protected String query;

    public QueryAdapter(ASTNode queryAST, String query) {
        this.queryAST = queryAST;
        this.query = query;
    }

    public QueryAdapter(ASTNode rootNode, boolean canonicalizeString) {
        this.queryAST = rootNode;
        String query = rootNode.toQueryString();
        if (canonicalizeString) {
            this.query = canonicalizeString(query);
        } else {
            this.query = query;
        }
    }

    public QueryAdapter(ASTNode queryAST) {
        this.queryAST = queryAST;
    }

    public QueryAdapter(String query, boolean canonicalizeString) {
        if (canonicalizeString) {
            this.query = canonicalizeString(query);
        } else {
            this.query = query;
        }
    }

    public QueryAdapter(String query) {
        this.query = query;
    }

    public ASTNode getQueryAST() {
        return queryAST;
    }

    @Override
    public String getQueryString() {
        return query;
    }

    private String canonicalizeString(String s) {
        if (s.endsWith(";")) {
            return s;
        } else if (!s.contains("--")) {
            return s + ";";
        } else {
            // query contains a comment
            return s;
        }
    }
}
