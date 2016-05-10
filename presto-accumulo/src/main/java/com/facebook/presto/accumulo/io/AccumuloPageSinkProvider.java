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
package com.facebook.presto.accumulo.io;

import com.facebook.presto.accumulo.AccumuloClient;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.model.AccumuloTableHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorSession;

import javax.inject.Inject;

import static com.facebook.presto.accumulo.Types.checkType;
import static java.util.Objects.requireNonNull;

/**
 * Page sink provider for Accumulo connector. Creates {@link AccumuloPageSink} objects for output
 * tables (CTAS) and inserts.
 *
 * @see AccumuloPageSink
 */
public class AccumuloPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final AccumuloClient client;
    private final AccumuloConfig config;

    /**
     * Creates a new instance of {@link AccumuloPageSinkProvider}
     *
     * @param config Connector configuration
     * @param client Client to pass along to the created page sinks
     */
    @Inject
    public AccumuloPageSinkProvider(AccumuloConfig config, AccumuloClient client)
    {
        this.client = requireNonNull(client, "client is null");
        this.config = requireNonNull(config, "config is null");
    }

    /**
     * Creates a page sink for the given output table handle
     *
     * @param session Current client session
     * @param outputTableHandle Output table handle
     * @return A new page sink
     */
    @Override
    public ConnectorPageSink createPageSink(ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle)
    {
        AccumuloTableHandle tHandle =
                checkType(outputTableHandle, AccumuloTableHandle.class, "tHandle");
        return new AccumuloPageSink(config, client.getTable(tHandle.toSchemaTableName()));
    }

    /**
     * Creates a page sink for the given insert table handle
     *
     * @param session Current client session
     * @param insertTableHandle Insert table handle
     * @return A new page sink
     */
    @Override
    public ConnectorPageSink createPageSink(ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle)
    {
        AccumuloTableHandle tHandle =
                checkType(insertTableHandle, AccumuloTableHandle.class, "tHandle");
        return new AccumuloPageSink(config, client.getTable(tHandle.toSchemaTableName()));
    }
}
