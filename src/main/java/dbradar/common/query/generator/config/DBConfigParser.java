package dbradar.common.query.generator.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.jse.JsePlatform;

public class DBConfigParser {

    private Globals luaGlobals;

    public DBConfigParser(String configPath) {
        luaGlobals = JsePlatform.standardGlobals();
        luaGlobals.loadfile(configPath).call();
    }

    /**
     * Extract database config information from a Lua variable, similar to the
     * following code piece: tables = {rows = {10, 20, 30, 90}, charsets = {'utf8',
     * 'latin1'}}
     *
     * @param varName The variable name for a key-value list, e.g., "tables"
     * @return The options for all properties, e.g., "rows" and "charsets"
     */
    public Map<String, List<String>> extractConfig(String varName) {
        try {
            LuaValue var = luaGlobals.get(LuaValue.valueOf(varName));

            Map<String, List<String>> config = new HashMap<>();

            if (var.isnil()) {
                throw new RuntimeException("Check the variable name: " + varName);
            }

            LuaValue propIndex = LuaValue.NIL; // From the start of properties
            while (true) {
                Varargs prop = var.next(propIndex);
                propIndex = prop.arg(1);
                if (propIndex.isnil()) {
                    break;
                }
                LuaValue varOptions = prop.arg(2);

                List<String> options = new ArrayList<>();
                LuaValue optionIndex = LuaValue.NIL; // From the start of options
                while (true) {
                    Varargs option = varOptions.next(optionIndex);
                    optionIndex = option.arg(1);
                    if (optionIndex.isnil()) {
                        break;
                    }
                    options.add(option.arg(2).toString());
                }

                config.put(propIndex.toString(), options);
            }

            return config;
        } catch (Exception e) {
            throw new RuntimeException("Database config parse fails.");
        }
    }
}
