package com.aliyun.dts.deliver.test;

import com.aliyun.dts.deliver.DtsMessageInterceptor;
import com.aliyun.dts.deliver.base.Source;
import com.aliyun.dts.deliver.commons.concurrency.VoidCallable;
import com.aliyun.dts.deliver.commons.config.Settings;
import com.aliyun.dts.deliver.commons.util.AutoCloseableIterator;
import com.aliyun.dts.deliver.commons.util.DefaultAutoCloseableIterator;
import com.aliyun.dts.deliver.protocol.generated.ConfiguredDtsCatalog;
import com.aliyun.dts.deliver.protocol.generated.ConnectorSpecification;
import com.aliyun.dts.deliver.protocol.generated.DtsCatalog;
import com.aliyun.dts.deliver.protocol.generated.DtsConnectionStatus;
import com.aliyun.dts.deliver.protocol.generated.DtsMessage;
import com.aliyun.dts.deliver.protocol.record.OperationType;
import com.aliyun.dts.deliver.protocol.record.Record;
import com.aliyun.dts.deliver.protocol.record.checkpoint.RecordCheckpoint;
import com.aliyun.dts.deliver.protocol.record.impl.DefaultRecord;
import com.aliyun.dts.deliver.protocol.record.impl.RecordHeader;
import com.aliyun.dts.deliver.protocol.record.impl.ddl.DDLRecord;
import com.aliyun.dts.deliver.recordinterceptor.dmlfilter.FilterRecordInterceptor;
import com.aliyun.dts.deliver.test.util.DtsRecordTestUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.metrics.Metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class FakeSource implements Source {

//    public static final String DB_NAME1 = "dts_deliver_test";
//    public static final String TABLE_NAME1 = "tab1";
//    public static final long GROUP_KEY1 = 100;
//
//    public static final String DB_NAME2 = "dts_deliver_test";
//    public static final String TABLE_NAME2 = "tab2";
//    public static final long GROUP_KEY2 = 200;
//
//    protected static final String DB_NAME3 = "dts_deliver_test";
//    protected static final String TABLE_NAME3 = "tab3";
//    protected static final long GROUP_KEY3 = 300;

    private String dbName;
    private String tableName;
    private long groupName;

    private String name;
    private Settings settings;

    private AtomicLong recordOffset = new AtomicLong();

    public FakeSource(String name, Settings settings, String dbName, String tableName, long groupName) {
        this.name = name;
        this.settings = settings;

        this.dbName = dbName;
        this.tableName = tableName;
        this.groupName = groupName;
    }

    @Override
    public String uniqueName() {
        return name;
    }

    @Override
    public RecordCheckpoint startCheckpoint() {
        return new MockRecordCheckpoint("123@abc");
    }

    @Override
    public DtsCatalog discover(JsonNode config) throws Exception {
        return null;
    }

    @Override
    public AutoCloseableIterator<DtsMessage> read(Settings settings, ConfiguredDtsCatalog catalog, JsonNode state) throws Exception {
        long timestamp = System.currentTimeMillis() / 1000 + + recordOffset.getAndIncrement();

        //DDL Record
        RecordHeader recordHeader = new RecordHeader(name, String.valueOf(timestamp));
        recordHeader.setSourceTimestamp(timestamp);
        recordHeader.setSource("MySQL");
        recordHeader.setVersion(1);

        String ddlSql = getCreateTableJsonSql(dbName, tableName);
        System.out.println("ddlSql: ");
        System.out.println(ddlSql);

        DefaultRecord createTableDDLRecord = DDLRecord.buildDDLRecord(ddlSql, dbName, tableName, new MockRecordCheckpoint("ddd@" + + recordOffset.getAndIncrement()), recordHeader);
        createTableDDLRecord.setGroupKey(groupName);

        //DML Record
        Record record1 = DtsRecordTestUtil.createRecord(new MockRecordCheckpoint("aaa@" + recordOffset.getAndIncrement()), groupName, name, dbName, tableName, OperationType.INSERT, timestamp,
                DtsRecordTestUtil.createField("id1", "1", null, true, false),
                DtsRecordTestUtil.createField("id2", "2", "uk1", false, false),
                DtsRecordTestUtil.createField("id3", "3", "uk2", false, false));

        Record record2 = DtsRecordTestUtil.createRecord(new MockRecordCheckpoint("bbb@" + recordOffset.getAndIncrement()), groupName, name, dbName, tableName, OperationType.INSERT, timestamp,
                DtsRecordTestUtil.createField("id1", "4", null, true, false),
                DtsRecordTestUtil.createField("id2", "5", "uk1", false, false),
                DtsRecordTestUtil.createField("id3", "6", "uk2", false, false));

        Record record3 = DtsRecordTestUtil.createRecord(new MockRecordCheckpoint("ccc@" + + recordOffset.getAndIncrement()), groupName, name, dbName, tableName, OperationType.INSERT, timestamp,
                DtsRecordTestUtil.createField("id1", "7", null, true, false),
                DtsRecordTestUtil.createField("id2", "8", "uk1", false, false),
                DtsRecordTestUtil.createField("id3", "9", "uk2", false, false));

        List<DtsMessage> dtsMessageList = new ArrayList<>();
        dtsMessageList.add(new DtsMessage().withType(DtsMessage.Type.RECORD).withRecord(createTableDDLRecord));

        //build heartbeat record
        Record heartbeat = DtsRecordTestUtil.createRecord(new MockRecordCheckpoint("aaa@" + recordOffset.getAndIncrement()),
                groupName, name, null, null, OperationType.HEARTBEAT, timestamp, null);

        dtsMessageList.add(new DtsMessage().withType(DtsMessage.Type.RECORD).withRecord(heartbeat));
        dtsMessageList.add(new DtsMessage().withType(DtsMessage.Type.RECORD).withRecord(heartbeat));

        dtsMessageList.add(new DtsMessage().withType(DtsMessage.Type.RECORD).withRecord(record1));
        dtsMessageList.add(new DtsMessage().withType(DtsMessage.Type.RECORD).withRecord(record2));
        dtsMessageList.add(new DtsMessage().withType(DtsMessage.Type.RECORD).withRecord(record3));

        //sleep 3s
        try {
            Thread.sleep(3000L);
        } catch (Exception e) {
        }

        return new DefaultAutoCloseableIterator(dtsMessageList.iterator(), VoidCallable.NOOP);
    }

    @Override
    public void open() throws Exception {

    }

    private String getCreateTableJsonSql(String dbName, String tableName) {
        return "\n" +
                "  {\n" +
                "    \"opType\": \"CREATE TABLE\",\n" +
                "    \"sdbType\": \"dameng\",\n" +
                "    \"sdbVersion\": \"5.6\",\n" +
                "    \"dbName\": \"" + dbName + "\",\n" +
                "    \"schemaName\": \"" + dbName + "\", \n" +
                "    \"tableName\": \"" + tableName + "\", \n" +
                "    \"columns\": [\n" +
                "      {\n" +
                "        \"name\": \"id1\",\n" +
                "        \"type\": {\n" +
                "          \"datatype\": \"varchar\",\n" +
                "          \"length\": 255\n" +
                "        },\n" +
                "        \"options\": {\n" +
                "          \"nullable\": false\n" +
                "        }\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"id2\",\n" +
                "        \"type\": {\n" +
                "          \"datatype\": \"varchar\",\n" +
                "          \"length\": 255\n" +
                "        },\n" +
                "        \"options\": {\n" +
                "          \"nullable\": true\n" +
                "        }\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"id3\",\n" +
                "        \"type\": {\n" +
                "          \"datatype\": \"varchar\",\n" +
                "          \"length\": 255\n" +
                "        },\n" +
                "        \"options\": {\n" +
                "          \"nullable\": true\n" +
                "        }\n" +
                "      }\n" +
                "    ],\n" +
                "    \"primaryKey\": {\n" +
                "      \"columns\": [\n" +
                "        {\n" +
                "          \"column\": \"id1\"\n" +
                "        }\n" +
                "      ],\n" +
                "      \"name\": \"pk1\"\n" +
                "    },\n" +
                "    \"uniqueKeys\": [\n" +
                "      {\n" +
                "        \"columns\": [\n" +
                "          {\n" +
                "            \"column\": \"id2\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"name\": \"unq_nick\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"indexes\": [\n" +
                "      {\n" +
                "        \"columns\": [\n" +
                "          {\n" +
                "            \"column\": \"id3\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"name\": \"ind_dd\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"options\": {\n" +
                "      \"comment\": \"for deliver test\",\n" +
                "      \"engine\": \"xxx\"\n" +
                "    }\n" +
                "  }";
    }

    @Override
    public List<DtsMessageInterceptor> recordInterceptors(Metrics metrics) {
        List<DtsMessageInterceptor> recordInterceptors = new LinkedList<>();

        // filter record interceptor is always enabled
        recordInterceptors.add(new FilterRecordInterceptor(settings, metrics, name));
        return Collections.emptyList();
    }

    @Override
    public ConnectorSpecification spec() throws Exception {
        return null;
    }

    @Override
    public DtsConnectionStatus check(JsonNode config) throws Exception {
        return null;
    }

    @Override
    public void close() {

    }


}
