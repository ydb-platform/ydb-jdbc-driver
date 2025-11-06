package tech.ydb.jdbc.query.params;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import tech.ydb.jdbc.YdbConst;
import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.common.YdbTypes;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.TupleType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.VoidValue;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class InListJdbcPrm {
    private static final Value<?> NULL = VoidValue.of();
    private final YdbTypes ydbTypes;
    private final String listName;
    private final List<Item> items = new ArrayList<>();
    private final Item[][] tuples;
    private final TypeDescription[] tupleTypes;

    public InListJdbcPrm(YdbTypes types, String listName, int listSize, int tupleSize) {
        this.ydbTypes = types;
        this.listName = listName;
        this.tupleTypes = new TypeDescription[tupleSize];
        this.tuples = new Item[listSize][];
        for (int idx = 0; idx < listSize; idx += 1) {
            Item[] tuple = new Item[tupleSize];
            for (int memberIdx = 0; memberIdx < tupleSize; memberIdx += 1) {
                tuple[memberIdx] = new Item(listName, idx * tupleSize + memberIdx, memberIdx);
                items.add(tuple[memberIdx]);
            }
            tuples[idx] = tuple;
        }
    }

    public List<? extends JdbcPrm> toJdbcPrmList() {
        return items;
    }

    private Value<?> buildList() throws SQLException {
        TypeBuilder[] types = new TypeBuilder[tupleTypes.length];
        for (int idx = 0; idx < tupleTypes.length; idx += 1) {
            if (tupleTypes[idx] == null) {
                throw new SQLException(YdbConst.MISSING_VALUE_FOR_PARAMETER + tuples[0][idx].name);
            }
            types[idx] = new TypeBuilder(tupleTypes[idx].ydbType());
        }

        for (Item item: items) {
            if (item.value == null) {
                throw new SQLException(YdbConst.MISSING_VALUE_FOR_PARAMETER + item.name);
            }
            types[item.memberId].validateOptional(item.value);
        }

        if (types.length == 1) { // Simple list
            TypeBuilder type = types[0];
            List<Value<?>> values = new ArrayList<>();
            for (Item item: items) {
                values.add(type.makeValue(item.value));
            }
            return ListType.of(type.makeType()).newValue(values);
        }

        Type[] tupleMemberTypes = new Type[tupleTypes.length];
        for (int idx = 0; idx < tupleTypes.length; idx += 1) {
            tupleMemberTypes[idx] = types[idx].makeType();
        }

        TupleType tupleType = TupleType.ofOwn(tupleMemberTypes);
        List<Value<?>> values = new ArrayList<>();
        for (Item[] tupleItems : tuples) {
            Value<?>[] tupleValues = new Value<?>[tupleItems.length];
            for (int idx = 0; idx < tupleItems.length; idx += 1) {
                tupleValues[idx] = types[idx].makeValue(tupleItems[idx].value);
            }
            values.add(tupleType.newValueOwn(tupleValues));
        }

        return ListType.of(tupleType).newValue(values);
    }

    private class TypeBuilder {
        private final Type type;
        private final OptionalType optional;

        private boolean isOptional = false;

        TypeBuilder(Type type) {
            this.type = type;
            this.optional = type.makeOptional();
        }

        void validateOptional(Value<?> value) {
            this.isOptional = isOptional || value == NULL;
        }

        Value<?> makeValue(Value<?> value) {
            if (!isOptional) {
                return value;
            }

            if (value == NULL) {
                return optional.emptyValue();
            }

            return value.makeOptional();
        }

        Type makeType() {
            return isOptional ? optional : type;
        }
    }

    private class Item implements JdbcPrm {
        private final String name;
        private final int index;
        private final int memberId;
        private Value<?> value = null;

        Item(String listName, int index, int tupleIdx) {
            this.name = listName + "[" + index + "]";
            this.index = index;
            this.memberId = tupleIdx;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public TypeDescription getType() {
            return tupleTypes[memberId];
        }

        @Override
        public void setValue(Object obj, int sqlType) throws SQLException {
            if (tupleTypes[memberId] == null) {
                Type ydbType = ydbTypes.findType(obj, sqlType);
                if (ydbType == null) {
                    if (obj == null) {
                        value = NULL;
                        return;
                    } else {
                        throw new SQLException(String.format(YdbConst.PARAMETER_TYPE_UNKNOWN, sqlType, obj));
                    }
                }

                tupleTypes[memberId] = ydbTypes.find(ydbType);
            }

            if (obj == null) {
                value = NULL;
                return;
            }

            value = tupleTypes[memberId].toYdbValue(obj);
        }

        @Override
        public void copyToParams(Params params) throws SQLException {
            if (index == 0) { // first prm
                params.put(listName, buildList());
            }
        }

        @Override
        public void reset() {
            value = null;
            if (index == items.size() - 1) { // last prm reset type
                Arrays.fill(tupleTypes, null);
            }
        }
    }
}
