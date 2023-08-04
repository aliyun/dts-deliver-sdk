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

public class ColumnMapper implements Mapper {

    private Map<String, String> columns = new HashMap<String, String>();

    public void addColumn(String original, String image) {
        this.columns.put(original, image);
    }

    public void initialize(String expressions) {
    }

    public String mapper(String original) {
        String column = columns.get(original);

        return column == null ? original : column;
    }

    public boolean contains(String original) {
        String column = columns.get(original);

        return column != null;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.columns.size());
        for (Map.Entry<String, String> entry : this.columns.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            this.columns.put(in.readUTF(), in.readUTF());
        }
    }
}
