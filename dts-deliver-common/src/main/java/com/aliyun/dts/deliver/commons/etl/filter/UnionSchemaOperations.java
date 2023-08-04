package com.aliyun.dts.deliver.commons.etl.filter;

import java.util.ArrayList;
import java.util.List;

public class UnionSchemaOperations implements SchemaOperations {
    private List<SchemaOperations> operations = new ArrayList<SchemaOperations>();

    public void addSchemaOperation(SchemaOperations condition) {
        if (condition != null) {
            this.operations.add(condition);
        }
    }

    @Override
    public String dmlOperations(String schema, String database, String table) {
        for (SchemaOperations operation : this.operations) {
            String value = operation.dmlOperations(schema, database, table);
            if (value != null) {
                return value;
            }
        }

        return null;
    }

    @Override
    public String ddlOperations(String schema, String database, String table) {
        for (SchemaOperations operation : this.operations) {
            String value = operation.ddlOperations(schema, database, table);
            if (value != null) {
                return value;
            }
        }

        return null;
    }
}
