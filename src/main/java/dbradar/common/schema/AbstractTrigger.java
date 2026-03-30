package dbradar.common.schema;

public class AbstractTrigger {

    private String name;

    public AbstractTrigger(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
