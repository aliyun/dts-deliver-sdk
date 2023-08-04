package com.aliyun.dts.deliver.commons.etl.filter;

public class RegularSchemaFilter extends SchemaFilter {

    private String database;
    private String table;
    private String column;

    public RegularSchemaFilter(String database, String table, String column, boolean casesensitived) {
        this(database, table, column, FilterMode.WHITELIST);
    }

    public RegularSchemaFilter(String database, String table, String column, FilterMode filterMode) {
        super(filterMode);

        this.database = database;
        this.table = table;
        this.column = column;

        if (!this.database.endsWith("$") && !this.database.endsWith(".*")) {
            this.database = this.database + ".*";
        }
        if (!this.database.startsWith("^") && !this.database.startsWith(".*")) {
            this.database = ".*" + this.database;
        }
        if (!this.table.endsWith("$") && !this.table.endsWith(".*")) {
            this.table = this.table + ".*";
        }
        if (!this.table.startsWith("^") && !this.table.startsWith(".*")) {
            this.table = ".*" + this.table;
        }
        if (!this.column.endsWith("$") && !this.column.endsWith(".*")) {
            this.column = this.column + ".*";
        }
        if (!this.column.startsWith("^") && !this.column.startsWith(".*")) {
            this.column = ".*" + this.column;
        }
    }

    @Override
    public boolean shouldIgnore(String schema, String database) {
        checkShouldNotBeNull(schema, database);

        if (FilterMode.WHITELIST == filterMode) {
            return !database.matches(this.database);
        } else {
            return database.matches(this.database) && ".*".equals(this.table);
        }
    }

    @Override
    public boolean shouldIgnore(String schema, String database, String table) {
        checkShouldNotBeNull(schema, database, table);

        if (FilterMode.WHITELIST == filterMode) {
            return !database.matches(this.database) || !table.matches(this.table);
        } else {
            return database.matches(this.database) && table.matches(this.table) && ".*".equals(this.column);
        }
    }

    @Override
    public boolean shouldIgnore(String schema, String database, String table, String column) {
        checkShouldNotBeNull(schema, database, table, column);

        if (FilterMode.WHITELIST == filterMode) {
            return !database.matches(this.database) || !table.matches(this.table) || !column.matches(this.column);
        } else {
            return database.matches(this.database) && table.matches(this.table) && column.matches(this.column);
        }
    }

    @Override
    public String condition(String schema, String database, String table) {
        return null;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }
}
