package com.aliyun.dts.deliver.connector;

import com.aliyun.dts.deliver.base.Integration;
import com.aliyun.dts.deliver.commons.json.Jsons;
import com.aliyun.dts.deliver.commons.resources.MoreResources;
import com.aliyun.dts.deliver.protocol.generated.ConnectorSpecification;

public abstract class BaseConnector implements Integration {

    /**
     * By convention the spec is stored as a resource for java connectors. That resource is called
     * spec.json.
     *
     * @return specification.
     * @throws Exception - any exception.
     */
    @Override
    public ConnectorSpecification spec() throws Exception {
        // return a JsonSchema representation of the spec for the integration.
        final String resourceString = MoreResources.readResource("spec.json");
        return Jsons.deserialize(resourceString, ConnectorSpecification.class);
    }

}
