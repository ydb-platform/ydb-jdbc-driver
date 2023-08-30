package tech.ydb.jdbc.impl;

public class YdbPreparedStatementWithDataQueryImpl /*extends AbstractYdbDataQueryPreparedStatementImpl*/ {

//    private final DataQuery dataQuery;
//    private final PreparedConfiguration cfg;
//
//    public YdbPreparedStatementWithDataQueryImpl(YdbConnectionImpl connection,
//                                                 int resultSetType,
//                                                 YdbQuery query,
//                                                 DataQuery dataQuery) throws SQLException {
//        super(connection, resultSetType, query, dataQuery);
//        this.dataQuery = dataQuery;
//        this.cfg = asPreparedConfiguration(dataQuery.types());
//    }
//
//    @Override
//    protected Map<String, TypeDescription> getParameterTypes() {
//        Map<String, Type> source = dataQuery.types();
//        Map<String, TypeDescription> target = new LinkedHashMap<>(source.size());
//        for (Map.Entry<String, Type> entry : source.entrySet()) {
//            target.put(entry.getKey(), TypeDescription.of(entry.getValue()));
//        }
//        return target;
//    }
//
//    //
//
//    private TypeDescription getParameter(String name) throws YdbExecutionException {
//        TypeDescription description = cfg.descriptions.get(name);
//        if (description == null) {
//            throw new YdbExecutionException(PARAMETER_NOT_FOUND + name);
//        }
//        return description;
//    }
//
//    private static PreparedConfiguration asPreparedConfiguration(Map<String, Type> types) {
//        int count = types.size();
//
//        Map<String, TypeDescription> descriptions = new HashMap<>(count);
//        for (Map.Entry<String, Type> entry : types.entrySet()) {
//            descriptions.put(entry.getKey(), TypeDescription.of(entry.getValue()));
//        }
//
//        return new PreparedConfiguration(descriptions);
//    }
//
//    private static class PreparedConfiguration {
//        private final Map<String, TypeDescription> descriptions;
//
//        private PreparedConfiguration(Map<String, TypeDescription> descriptions) {
//            this.descriptions = descriptions;
//        }
//    }
}
