package tech.ydb.jdbc.query;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.common.TypeDescription;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class ParamDescription {
    private final String name;
    private final String displayName;
    private final TypeDescription type;

    public ParamDescription(String name, String displayName, TypeDescription type) {
        this.name = name;
        this.displayName = displayName;
        this.type = type;
    }

    public ParamDescription(String name, TypeDescription type) {
        this(name, YdbConst.VARIABLE_PARAMETER_PREFIX + name, type);
    }

    public String name() {
        return name;
    }

    public String displayName() {
        return displayName;
    }

    public TypeDescription type() {
        return type;
    }
}
