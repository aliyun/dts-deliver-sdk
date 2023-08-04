
package com.aliyun.dts.deliver.commons.etl.mapper;

public interface Mapper {

    void initialize(String expressions);

    String mapper(String original);

    boolean contains(String original);
}
