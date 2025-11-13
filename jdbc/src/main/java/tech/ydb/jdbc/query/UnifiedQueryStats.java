package tech.ydb.jdbc.query;

import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Data class to wrap QueryStats of Table and Query services
 */
public class UnifiedQueryStats {
    private final String queryPlan;
    private final String queryAst;
    private final long totalDurationUs;
    private final long totalCpuTimeUs;
    private final long processCpuTimeUs;
    private final long compilationTimeUs;
    private final long compilationCpuTimeUs;
    private final boolean compilationIsCached;
    private final List<PhaseStats> queryPhases;

    public UnifiedQueryStats(tech.ydb.query.result.QueryStats src) {
        this.queryPlan = src.getQueryPlan();
        this.queryAst = src.getQueryAst();
        this.totalDurationUs = src.getTotalDurationUs();
        this.totalCpuTimeUs = src.getTotalCpuTimeUs();
        this.processCpuTimeUs = src.getProcessCpuTimeUs();
        this.compilationTimeUs = src.getCompilationStats().getDurationUs();
        this.compilationCpuTimeUs = src.getCompilationStats().getCpuTimeUs();
        this.compilationIsCached = src.getCompilationStats().isFromCache();
        this.queryPhases = src.getPhases().stream().map(PhaseStats::new).collect(toList());
    }

    public UnifiedQueryStats(tech.ydb.table.query.stats.QueryStats src) {
        this.queryPlan = src.getQueryPlan();
        this.queryAst = src.getQueryAst();
        this.totalDurationUs = src.getTotalDurationUs();
        this.totalCpuTimeUs = src.getTotalCpuTimeUs();
        this.processCpuTimeUs = src.getProcessCpuTimeUs();
        this.compilationTimeUs = src.getCompilation().getDurationUs();
        this.compilationCpuTimeUs = src.getCompilation().getCpuTimeUs();
        this.compilationIsCached = src.getCompilation().getFromCache();
        this.queryPhases = src.getQueryPhasesList().stream().map(PhaseStats::new).collect(toList());
    }

    public String getQueryPlan() {
        return queryPlan;
    }

    public String getQueryAst() {
        return queryAst;
    }

    public long getTotalDurationUs() {
        return totalDurationUs;
    }

    public long getTotalCpuTimeUs() {
        return totalCpuTimeUs;
    }

    public long getProcessCpuTimeUs() {
        return processCpuTimeUs;
    }

    public long getCompilationTimeUs() {
        return compilationTimeUs;
    }

    public long getCompilationCpuTimeUs() {
        return compilationCpuTimeUs;
    }

    public boolean isCompilationIsCached() {
        return compilationIsCached;
    }

    public List<PhaseStats> getQueryPhases() {
        return queryPhases;
    }

    @Override
    public String toString() {
        return "UnifiedQueryStats{" +
                "queryPlan='" + queryPlan + '\'' +
                ", queryAst='" + queryAst + '\'' +
                ", totalDurationUs=" + totalDurationUs +
                ", totalCpuTimeUs=" + totalCpuTimeUs +
                ", processCpuTimeUs=" + processCpuTimeUs +
                ", compilationTimeUs=" + compilationTimeUs +
                ", compilationCpuTimeUs=" + compilationCpuTimeUs +
                ", compilationIsCached=" + compilationIsCached +
                ", queryPhases=" + queryPhases +
                '}';
    }

    public static class PhaseStats {
        private final long durationUs;
        private final long cpuTimeUs;
        private final long affectedShards;
        private final boolean isLiteralPhase;
        private final List<TableAccess> tableAccesses;

        public PhaseStats(tech.ydb.query.result.QueryStats.QueryPhase src) {
            this.durationUs = src.getDurationUs();
            this.cpuTimeUs = src.getCpuTimeUs();
            this.affectedShards = src.getAffectedShards();
            this.isLiteralPhase = src.isLiteralPhase();
            this.tableAccesses = src.getTableAccesses().stream().map(TableAccess::new).collect(toList());
        }

        public PhaseStats(tech.ydb.table.query.stats.QueryPhaseStats src) {
            this.durationUs = src.getDurationUs();
            this.cpuTimeUs = src.getCpuTimeUs();
            this.affectedShards = src.getAffectedShards();
            this.isLiteralPhase = src.getLiteralPhase();
            this.tableAccesses = src.getTableAccessList().stream().map(TableAccess::new).collect(toList());
        }

        public long getDurationUs() {
            return durationUs;
        }

        public long getCpuTimeUs() {
            return cpuTimeUs;
        }

        public long getAffectedShards() {
            return affectedShards;
        }

        public boolean isLiteralPhase() {
            return isLiteralPhase;
        }

        public List<TableAccess> getTableAccesses() {
            return tableAccesses;
        }

        @Override
        public String toString() {
            return "PhaseStats{" +
                    "durationUs=" + durationUs +
                    ", cpuTimeUs=" + cpuTimeUs +
                    ", affectedShards=" + affectedShards +
                    ", isLiteralPhase=" + isLiteralPhase +
                    ", tableAccesses=" + tableAccesses +
                    '}';
        }

        public static class TableAccess {
            private final String name;
            private final long partitionsCount;
            private final TableOperation reads;
            private final TableOperation updates;
            private final TableOperation deletes;

            public TableAccess(tech.ydb.query.result.QueryStats.TableAccess src) {
                this.name = src.getTableName();
                this.partitionsCount = src.getPartitionsCount();
                this.reads = new TableOperation(src.getReads().getRows(), src.getReads().getBytes());
                this.updates = new TableOperation(src.getUpdates().getRows(), src.getUpdates().getBytes());
                this.deletes = new TableOperation(src.getDeletes().getRows(), src.getDeletes().getBytes());
            }

            public TableAccess(tech.ydb.table.query.stats.TableAccessStats src) {
                this.name = src.getName();
                this.partitionsCount = src.getPartitionsCount();
                this.reads = new TableOperation(src.getReads().getRows(), src.getReads().getBytes());
                this.updates = new TableOperation(src.getUpdates().getRows(), src.getUpdates().getBytes());
                this.deletes = new TableOperation(src.getDeletes().getRows(), src.getDeletes().getBytes());
            }

            public String getName() {
                return name;
            }

            public long getPartitionsCount() {
                return partitionsCount;
            }

            public TableOperation getReads() {
                return reads;
            }

            public TableOperation getUpdates() {
                return updates;
            }

            public TableOperation getDeletes() {
                return deletes;
            }

            @Override
            public String toString() {
                return "TableAccess{" +
                        "name='" + name + '\'' +
                        ", partitionsCount=" + partitionsCount +
                        ", reads=" + reads +
                        ", updates=" + updates +
                        ", deletes=" + deletes +
                        '}';
            }

            public static class TableOperation {
                private final long rows;
                private final long bytes;

                public TableOperation(long rows, long bytes) {
                    this.rows = rows;
                    this.bytes = bytes;
                }

                public long getRows() {
                    return rows;
                }

                public long getBytes() {
                    return bytes;
                }

                @Override
                public String toString() {
                    return "<rows=" + rows +
                            ", bytes=" + bytes +
                            '>';
                }
            }
        }
    }
}
