package dbradar.common.query;

public class QueryConfig {

    private String queryRoot;
    private boolean canAffectSchema;
    private boolean canBeRetried;

    public QueryConfig(String queryRoot, boolean canAffectSchema, boolean canBeRetried) {
        this.queryRoot = queryRoot;
        this.canAffectSchema = canAffectSchema;
        this.canBeRetried = canBeRetried;
    }

    public QueryConfig(String queryRoot, boolean canAffectSchema) {
        this(queryRoot, canAffectSchema, true);
    }

    public String getQueryRoot() {
        return queryRoot;
    }

    public boolean canAffectSchema() {
        return canAffectSchema;
    }

    public boolean canBeRetried() {
        return canBeRetried;
    }
}
