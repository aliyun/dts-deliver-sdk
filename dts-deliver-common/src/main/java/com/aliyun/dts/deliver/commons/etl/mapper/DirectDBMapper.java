/**
 *  (C) 2010-2014 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.dts.deliver.commons.etl.mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DirectDBMapper implements SchemaMapper {

    public static final String ALL = "*";
    public static final String AMP_MAPPER_DB_EXPRESSIONS = "amp.mapper.db.expressions";

    private Map<String, Mapper> columns = new HashMap<String, Mapper>();

    private Map<String, String> maps = new HashMap<String, String>();

    public DirectDBMapper() {
    }

    public DirectDBMapper(boolean normalLowerCase) {
    }

    public void initialize(String expressions) {
        if (expressions == null || expressions.trim().length() <= 0) {
            return;
        }

        String[] ss = expressions.split("\\|");
        for (String s : ss) {
            parseExpression(s.trim());
        }
    }

    protected void parseExpression(String expression) {
        String[] ss = expression.split("=");
        if (ss.length != 2) {
            return;
        }

        String source = ss[0].trim();
        String target = ss[1].trim();

        String sDb = null;
        String sTable = null;
        String sColumn = null;

        String tDb = null;
        String tTable = null;
        String tColumn = null;

        String[] sources = source.split(";");
        String[] targets = target.split(";");
        switch (sources.length) {
            case 3:
                sColumn = sources[2];
            case 2:
                sTable = sources[1];
            case 1:
                sDb = sources[0];
            default:
        }
        switch (targets.length) {
            case 3:
                tColumn = targets[2];
            case 2:
                tTable = targets[1];
            case 1:
                tDb = targets[0];
            default:
        }

        // if(s_db != null && t_db != null)
        // this.maps.put(s_db, t_db);
        //
        // if(s_table != null && t_table != null)
        // this.maps.put(s_db + "." + s_table, t_table);
        //
        // if(s_column != null && t_column != null){
        // String key = s_db + "." + s_table;
        // ColumnMapper mapper = (ColumnMapper) this.columns.get(key);
        // if(mapper == null){
        // mapper = new ColumnMapper();
        // this.columns.put(key, mapper);
        // }
        //
        // mapper.addColumn(s_column, t_column);
        // }

        if (sDb != null && tDb != null) {
            this.maps.put(sDb, tDb);
        }

        if (sTable != null && tTable != null) {
            this.maps.put(sDb + "." + sTable, tTable);
        }

        if (sColumn != null && tColumn != null) {
            String key = sDb + "." + sTable;
            ColumnMapper mapper = (ColumnMapper) this.columns.get(key);
            if (mapper == null) {
                mapper = new ColumnMapper();
                this.columns.put(key, mapper);
            }

            mapper.addColumn(sColumn, tColumn);
        } else if (sTable != null && tTable != null) {
            this.maps.put(sDb + "." + sTable, tTable);
        } else if (sDb != null && tDb != null) {
            this.maps.put(sDb, tDb);
        }
    }

    public void write(DataOutput out) throws IOException {
    }

    public void readFields(DataInput in) throws IOException {
    }

    public String mapper(String database) {
        String newDB = maps.get(database);

        return newDB == null ? database : newDB;
    }

    public String mapper(String database, String table) {
        String key = database + "." + table;
        String newTable = maps.get(key);

        return newTable == null ? table : newTable;
    }

    public boolean contains(String original) {
        return maps.get(original) != null;
    }

    public Mapper getColumnMapper(String database, String table) {
        return this.columns.get(database + "." + table);
    }

    static class Table {
        String database;
        String table;

        String value;

        Table(String database, String table, String value) {
            this.database = database;
            this.table = table;
            this.value = value;
        }
    }
}
