package com.aliyun.dts.deliver.protocol.record;

public interface RawDataType {

    String getTypeName();

    int getTypeId();

    boolean isLobType();
}
