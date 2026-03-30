package dbradar.sqlite3.schema;

import dbradar.common.schema.AbstractTrigger;

public class SQLite3Trigger extends AbstractTrigger {

    private boolean isTemporary;

    public SQLite3Trigger(String name, boolean isTemporary) {
        super(name);
        this.isTemporary = isTemporary;
    }

    public void setTemporary(boolean temporary) {
        isTemporary = temporary;
    }

    public boolean isTemporary() {
        return isTemporary;
    }
}
