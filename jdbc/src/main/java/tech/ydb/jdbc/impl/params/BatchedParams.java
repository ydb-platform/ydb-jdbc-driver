package tech.ydb.jdbc.impl.params;


import java.util.Map;

import tech.ydb.jdbc.common.TypeDescription;
import tech.ydb.jdbc.impl.YdbJdbcParams;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class BatchedParams implements YdbJdbcParams {
    private final Map<String, TypeDescription> types;

    private BatchedParams(String listName, ListType listType, StructType itemType) {

    }

    public static BatchedParams fromDataQueryTypes(Map<String, Type> types) {
        // Only single parameter
        if (types.size() != 1) {
            return null;
        }

        String listName = types.keySet().iterator().next();
        Type type = types.get(listName);

        // Only list of values
        if (type.getKind() != Type.Kind.LIST) {
            return null;
        }

        ListType listType = (ListType) type;
        Type innerType = listType.getItemType();

        // Component - must be struct (i.e. list of structs)
        if (innerType.getKind() != Type.Kind.STRUCT) {
            return null;
        }

        StructType itemType = (StructType)innerType;
        return new BatchedParams(listName, listType, itemType);
    }

//    private static StructBatchConfiguration fromStruct(String paramName, ListType listType, StructType structType) {
//        int membersCount = structType.getMembersCount();
//
//        Map<String, TypeDescription> types = new LinkedHashMap<>(membersCount);
//        Map<String, Integer> indexes = new HashMap<>(membersCount);
//        String[] names = new String[membersCount];
//        TypeDescription[] descriptions = new TypeDescription[membersCount];
//        for (int i = 0; i < membersCount; i++) {
//            String name = "$" + structType.getMemberName(i);
//            Type type = structType.getMemberType(i);
//            TypeDescription description = TypeDescription.of(type);
//            if (indexes.put(name, i) != null) {
//                throw new IllegalStateException("Internal error. YDB must not bypass this struct " +
//                        "with duplicate member " + paramName);
//            }
//            types.put(name, description);
//            names[i] = name;
//            descriptions[i] = description;
//        }
//        return new StructBatchConfiguration(paramName, listType, structType, types, indexes, names, descriptions);
//    }
}
