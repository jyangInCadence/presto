/*
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
package com.facebook.presto.accumulo;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Factory for creating an {@link AccumuloConnector}
 */
public class AccumuloConnectorFactory
        implements ConnectorFactory
{
    public static final String CONNECTOR_NAME = "accumulo";
    private final TypeManager typeManager;
    private final Map<String, String> optionalConfig;

    public AccumuloConnectorFactory(TypeManager typeManager, Map<String, String> optionalConfig)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.optionalConfig =
                ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
    }

    /**
     * Gets the name of the connector, "accumulo"
     *
     * @return Connector name
     */
    @Override
    public String getName()
    {
        return CONNECTOR_NAME;
    }

    /**
     * Creates an instance of the connector with the given ID and required configuration values
     *
     * @param connectorId Connector ID
     * @param requiredConfig Required configuration parameters
     */
    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfig)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(requiredConfig, "requiredConfig is null");

        try {
            // A plugin is not required to use Guice; it is just very convenient
            // Unless you don't really know how to Guice, then it is less convenient
            Bootstrap app =
                    new Bootstrap(new JsonModule(), new AccumuloModule(connectorId, typeManager));

            Injector injector = app.strictConfig().doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .setOptionalConfigurationProperties(optionalConfig).initialize();

            return injector.getInstance(AccumuloConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new AccumuloHandleResolver();
    }
}
