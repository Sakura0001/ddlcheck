package dbradar.common.query.generator;

import java.util.HashMap;
import java.util.Map;

import dbradar.GlobalState;

public abstract class AbstractKeyFuncManager {

    protected GlobalState globalState;
    protected QueryContext currentContext;

    protected Map<String, KeyFunc> keyFuncMap = new HashMap<>();

    public AbstractKeyFuncManager(GlobalState globalState) {
        this.globalState = globalState;
        currentContext = new QueryContext();
    }

    public KeyFunc getFuncByKey(String key) {
        if (keyFuncMap.containsKey(key)) {
            KeyFunc func = keyFuncMap.get(key);
            if (func != null) {
                return func;
            }
        }
        throw new RuntimeException("KeyFuncManager: Failed to get key function for " + key);
    }

    public QueryContext getCurrentContext() {
        return currentContext;
    }

    public void cleanupFillers() {
        // Fix all fillers in the default context. This method can only be invoked when
        // the AST has been generated.
        // For example, SELECT ca1 FROM (SELECT c1 as ca1 FROM t1).
        currentContext.fillColumnReferences();
    }
}
