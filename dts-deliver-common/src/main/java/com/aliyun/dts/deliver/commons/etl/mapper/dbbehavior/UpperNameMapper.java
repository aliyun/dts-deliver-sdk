package com.aliyun.dts.deliver.commons.etl.mapper.dbbehavior;

import org.apache.commons.lang3.StringUtils;

public class UpperNameMapper implements NameMapper {

    @Override
    public String toTarget(String sourceName) {
        if (StringUtils.isEmpty(sourceName)) {
            return sourceName;
        }
        return sourceName.toUpperCase();
    }
}
