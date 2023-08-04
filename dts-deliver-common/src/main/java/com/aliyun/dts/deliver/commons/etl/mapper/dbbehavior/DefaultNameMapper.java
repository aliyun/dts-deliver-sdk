
package com.aliyun.dts.deliver.commons.etl.mapper.dbbehavior;

public class DefaultNameMapper implements NameMapper {

    @Override
    public String toTarget(String sourceName) {
        return sourceName;
    }
}
