package com.aliyun.dts.deliver.recordinterceptor.dmlfilter;

import com.aliyun.dts.deliver.DtsMessageInterceptor;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.etl.ETLInstance;
import com.aliyun.dts.deliver.commons.etl.filter.SchemaFilter;
import com.aliyun.dts.deliver.commons.exceptions.CriticalDtsException;
import com.aliyun.dts.deliver.commons.exceptions.ErrorCode;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import com.aliyun.dts.deliver.protocol.record.OperationType;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.util.RecordTools;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FilterRecordInterceptor implements DtsMessageInterceptor {
    private static final Logger LOG = LoggerFactory.getLogger(FilterRecordInterceptor.class);
    public static final Settings.Setting<Boolean> FILTER_INTERCEPTOR_SUPPORT_DDL =
            Settings.booleanSetting("amp.increment.ddl.support",
                    "if ddl is supported",
                    true);
    public static final Settings.Setting<Boolean> FILTER_INTERCEPTOR_IGNORE_DDL =
            Settings.booleanSetting("amp.increment.ddl.ignore",
                    "if ddl is not supported, if it should be ignored",
                    false);
    public static final Settings.Setting<Boolean> FILTER_INTERCEPTOR_FAKE_DDL =
            Settings.booleanSetting("amp.increment.ddl.fake",
                    "if ddl is not supported, if it not be ignored, if it should be faked",
                    false);

    public static final Settings.Setting<Boolean> FILTER_INTERCEPTOR_FILTER_ALL_DDL =
            Settings.booleanSetting("filterDDL", "if all ddl should be filtered",
                    false);

    private Map<String, DBInformation> databases = new HashMap<>();
    private final boolean trace = false;
    ETLInstance etlInstance;
    SchemaFilter unionSchemaFilter;
    private boolean support = true;
    private boolean ignore = false;
    private boolean allCreateTable = false;
    private Metrics metrics;

    private FilterMetric filterMetric;

    private boolean filterAllDDL = false;

    private String sourceName;

    public FilterRecordInterceptor(Settings settings, Metrics metrics, String sourceName) {
        this.etlInstance = new ETLInstance(settings);
        unionSchemaFilter = etlInstance.getSchemaFilter();
        this.metrics = metrics;
        this.filterMetric = new FilterMetric(metrics, sourceName);
        filterAllDDL = FILTER_INTERCEPTOR_FILTER_ALL_DDL.getValue(settings);
        if (filterAllDDL) {
            this.support = false;
            this.ignore = true;
        } else {
            this.support = FILTER_INTERCEPTOR_SUPPORT_DDL.getValue(settings);
            this.ignore = FILTER_INTERCEPTOR_IGNORE_DDL.getValue(settings);
        }

        this.sourceName = sourceName;

        LOG.info("FilterRecordInterceptor: support ddl [{}], ignore ddl [{}]", support, ignore);
    }

    @Override
    public String name() {
        return "FilterRecordInterceptor(filter dml by white list)";
    }

    @Override
    public void initialize(Settings settings) {
    }

    @Override
    public List<DtsMessage> intercept(List<DtsMessage> dtsMessages) {
        List<DtsMessage> outputRecords = new ArrayList<>(dtsMessages.size());
        int affectedRecords = 0;

        for (DtsMessage dtsMessage : dtsMessages) {
            DtsMessage newRecord = intercept(dtsMessage);
            if (null != newRecord) {
                outputRecords.add(newRecord);

                if (newRecord.getRecord() != null) {
                    switch (newRecord.getRecord().getOperationType()) {
                        case BEGIN:
                        case COMMIT:
                        case HEARTBEAT:
                            affectedRecords++;
                        default:
                            break;
                    }
                }
            }
        }
        return outputRecords;
    }

    @Override
    public DtsMessage intercept(DtsMessage dtsMessage) {
        filterMetric.incTotal();

        if (dtsMessage.getType() != DtsMessage.Type.RECORD) {
            return dtsMessage;
        }

        Record record = dtsMessage.getRecord();

        filterMetric.setLatestRecordTimestamp(record.getTimestamp());
        OperationType type = record.getOperationType();
        switch (type) {
            case HEARTBEAT: {
                LOG.info("Heartbeat:id(" + record.getId() + ")|ts(" + record.getTimestamp() + "), " + filterMetric);
                return dtsMessage;
            }
            case BEGIN:
            case COMMIT: {
                if (this.trace) {
                    LOG.info("hit Record: " + record.getId() + "/" + record.getTransactionId() + "/" + record.getOperationType());
                }
                return dtsMessage;
            }
            case INSERT:
            case UPDATE:
            case DELETE: {
                //available ++;
                String database = RecordTools.getDBName(record);
                String schema = RecordTools.getSchemaName(record);
                String table = RecordTools.getTableName(record);

                DBInformation dbInfo = getDBInformation(database, schema);
                TableInformation tbInfo = getTableInformation(dbInfo, database, schema, table);
                filterMetric.incReceiveDML();
                if (!tbInfo.filter(record)) {
                    if (this.trace) {
                        LOG.info("hit Record: " + record.getId() + "/" + record.getTransactionId() + "/" + record.getOperationType());
                    }
                    filterMetric.incHintDML();
                    return dtsMessage;
                } else {
                    if (this.trace) {
                        LOG.info("filter record: " + record.getId() + "/" + record.getTransactionId() + "/" + record.getOperationType());
                    }
                    return null;
                }
            }
            case DDL: {
                filterMetric.incReceiveDDL();
                if (!this.support) {
                    if (!this.ignore) {
                        throw new UnsupportedOperationException("Can not support the DDL [" + record.getCheckpoint() + "@" + record.getId() + "]");
                    } else {
                        LOG.warn("Ignore the DDL record: \n" + record);
                    }

                    if (this.trace) {
                        LOG.info("filter record: " + record.getId() + "/" + record.getTransactionId() + "/" + record.getOperationType());
                    }
                    return null;
                } else {
                    if (this.shouldIgnoreThisDDL(record)) {
                        LOG.info("**** Filter a DDL record: \n" + record);

                        if (this.trace) {
                            LOG.info("filter record: " + record.getId() + "/" + record.getTransactionId() + "/" + record.getOperationType());
                        }
                        return null;
                    } else {
                        LOG.info("**** Hit a DDL record: \n" + record);
                        filterMetric.incHinDDL();
                        if (this.trace) {
                            LOG.info("hit record: " + record.getId() + "/" + record.getTransactionId() + "/" + record.getOperationType());
                        }
                        // we do mapper in this step
                        return dtsMessage;
                    }
                }
            }
            default: {
                throw new CriticalDtsException("capture-kafka", ErrorCode.CAPTURE_UNSUPPORTED_RECORD_TYPE,
                        "not support " + record + " to filter");
            }
        }
    }

    private boolean shouldIgnoreThisDDL(Record record) {
        return false;
    }

    public DBInformation getDBInformation(String database, String schema) {
        String key = database + "." + schema;

        DBInformation dbInfo = this.databases.get(key);
        if (dbInfo != null) {
            return dbInfo;
        }

        if (unionSchemaFilter.shouldIgnore(schema, database)) {
            LOG.info("FilterRecordInterceptor: filter database: " + database + "." + schema);
            //dbInfo = new BlackDBInformation(database, schema);
            dbInfo = DBInformation.BLACK_DB_INFORMATION;
            this.databases.put(key, dbInfo);
            return dbInfo;
        }

        LOG.info("FilterRecordInterceptor: process database: " + database + "." + schema);

        //dbInfo = new DBInformation(database, schema);
        dbInfo = new DBInformation(database, aliasDatabase(database), schema, aliasSchema(schema));
        this.databases.put(key, dbInfo);
        return dbInfo;
    }

    public TableInformation getTableInformation(DBInformation dbInfo, String database, String schema, String table) {
        TableInformation tbInfo = dbInfo.getTableInformation(table);
        if (tbInfo != null) {
            return tbInfo;
        }

        //if(this.context.getSchemaManager().filter("[" + database + "]", "[" + database + "]", "[" + schema + "].[" + table + "]")){
        if (unionSchemaFilter.shouldIgnore(schema, database, table)) {
            LOG.info("FilterRecordInterceptor: filter table: " + database + "." + schema + "." + table);
            tbInfo = TableInformation.BLACK_TABLE_INFORMATION;
            dbInfo.addTable(table, tbInfo);
            return tbInfo;
        }

        String dmlOperations = unionSchemaFilter.dmlOperations(database, database, table);
        String ddlOperations = unionSchemaFilter.ddlOperations(database, database, table);
        LOG.info("FilterRecordInterceptor: process table: " + database + "." + table + "{" + dmlOperations + "}.{" + ddlOperations + "}");
        tbInfo = new TableInformation(database, schema, table, dbInfo.databaseAlias(), dbInfo.schemaAlias(),
                aliasTable(database, table), dmlOperations, ddlOperations);
        dbInfo.addTable(table, tbInfo);

        return tbInfo;
    }


    protected String aliasDatabase(String database) {
        return database;
    }

    protected String aliasSchema(String schema) {
        return schema;
    }

    protected String aliasTable(String database, String table) {
        return table;
    }
}
