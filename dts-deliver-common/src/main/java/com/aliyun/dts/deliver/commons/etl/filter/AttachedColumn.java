
package com.aliyun.dts.deliver.commons.etl.filter;

public class AttachedColumn {

    private final String name;
    private final String type;
    private final String value;

    public AttachedColumn(String name, String type, String value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public String name() {
        return this.name;
    }

    public String type() {
        return this.type;
    }

    public String value() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.name + "/" + this.type + "/" + this.value;
    }
}
