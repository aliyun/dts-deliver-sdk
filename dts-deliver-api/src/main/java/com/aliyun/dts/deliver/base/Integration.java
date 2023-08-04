package com.aliyun.dts.deliver.base;

import com.aliyun.dts.deliver.DtsMessageInterceptor;
import com.aliyun.dts.deliver.protocol.generated.ConnectorSpecification;
import com.aliyun.dts.deliver.protocol.generated.DtsConnectionStatus;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.metrics.Metrics;

import java.util.Collections;
import java.util.List;

public interface Integration {

    /**
     * Fetch the specification for the integration.
     *
     * @return specification.
     * @throws Exception - any exception.
     */
    ConnectorSpecification spec() throws Exception;

    /**
     * Check whether, given the current configuration, the integration can connect to the integration.
     *
     * @param config - integration-specific configuration object as json. e.g. { "username": "dts",
     *        "password": "super secure" }
     * @return Whether or not the connection was successful. Optional message if it was not.
     * @throws Exception - any exception.
     */
    DtsConnectionStatus check(JsonNode config) throws Exception;

    /**
     * Close the source, which means to release resources related, and cleanup the context
     *
     */
    void close();
}

