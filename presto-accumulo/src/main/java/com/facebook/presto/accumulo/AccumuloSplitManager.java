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

import com.facebook.presto.accumulo.model.AccumuloColumnConstraint;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.AccumuloSplit;
import com.facebook.presto.accumulo.model.AccumuloTableHandle;
import com.facebook.presto.accumulo.model.AccumuloTableLayoutHandle;
import com.facebook.presto.accumulo.model.TabletSplitMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.TupleDomain.ColumnDomain;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.accumulo.Types.checkType;
import static java.util.Objects.requireNonNull;

/**
 * Presto split manager for Accumulo. Main entry point for logically splitting an Accumulo table
 * into multiple splits for a parallel data read
 */
public class AccumuloSplitManager
        implements ConnectorSplitManager
{
    private static final Logger LOG = Logger.get(AccumuloSplitManager.class);

    private final String connectorId;
    private final AccumuloClient client;

    /**
     * Creates a new instance of an {@link AccumuloSplitManager}, injected by the Google.
     *
     * @param connectorId Connector ID
     * @param client Client object for retrieving table metadata and splits
     */
    @Inject
    public AccumuloSplitManager(AccumuloConnectorId connectorId, AccumuloClient client)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.client = requireNonNull(client, "client is null");
    }

    /**
     * Gets the source of splits from the given Presto table layout
     *
     * @param transactionHandle Transaction handle
     * @param session Current client session
     * @param layout Table layout
     * @return Split source to pass splits to Presto for scan
     */
    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        AccumuloTableLayoutHandle layoutHandle =
                checkType(layout, AccumuloTableLayoutHandle.class, "layout");
        AccumuloTableHandle tableHandle = layoutHandle.getTable();

        String schemaName = tableHandle.getSchema();
        String tableName = tableHandle.getTable();
        String rowIdName = tableHandle.getRowId();

        // Get non-row ID column constraints
        List<AccumuloColumnConstraint> constraints = getColumnConstraints(rowIdName, layoutHandle.getConstraint());

        // Get the row domain column range
        Optional<Domain> rDom = getRangeDomain(rowIdName, layoutHandle.getConstraint());

        // Call out to our client to retrieve all tablet split metadata using the row ID domain
        // and the secondary index, if enabled
        List<TabletSplitMetadata> tSplits = client.getTabletSplits(session, schemaName, tableName,
                rDom, constraints, tableHandle.getSerializerInstance());

        // Pack the tablet split metadata into a connector split
        ImmutableList.Builder<ConnectorSplit> cSplits = ImmutableList.builder();
        for (TabletSplitMetadata smd : tSplits) {
            AccumuloSplit split = new AccumuloSplit(connectorId, schemaName, tableName, rowIdName,
                    tableHandle.getSerializerClassName(), smd.getRanges(), constraints,
                    tableHandle.getScanAuthorizations(), smd.getHostPort());
            LOG.debug("Added split %s", split);
            cSplits.add(split);
        }

        // TODO Would this be more beneficial to return splits in batches?
        return new FixedSplitSource(connectorId, cSplits.build());
    }

    /**
     * Gets the Domain for the range, or null if there is no constraint on the row ID
     *
     * @param rowIdName Presto column of the row ID
     * @param constraint Query constraints
     * @return Domain on the row ID, or null if there is none
     */
    private static Optional<Domain> getRangeDomain(String rowIdName, TupleDomain<ColumnHandle> constraint)
    {
        for (ColumnDomain<ColumnHandle> cd : constraint.getColumnDomains().get()) {
            AccumuloColumnHandle col =
                    checkType(cd.getColumn(), AccumuloColumnHandle.class, "column handle");
            if (col.getName().equals(rowIdName)) {
                return Optional.of(cd.getDomain());
            }
        }

        return Optional.empty();
    }

    /**
     * Gets a list of {@link AccumuloColumnConstraint} based on the given constraint ID, excluding
     * the row ID column
     *
     * @param rowIdName Presto column name mapping to the Accumulo row ID
     * @param constraint Set of query constraints
     * @return List of all column constraints
     */
    private static List<AccumuloColumnConstraint> getColumnConstraints(String rowIdName,
            TupleDomain<ColumnHandle> constraint)
    {
        ImmutableList.Builder<AccumuloColumnConstraint> constraintBuilder = ImmutableList.builder();
        for (ColumnDomain<ColumnHandle> cd : constraint.getColumnDomains().get()) {
            AccumuloColumnHandle col =
                    checkType(cd.getColumn(), AccumuloColumnHandle.class, "column handle");

            if (!col.getName().equals(rowIdName)) {
                // Family and qualifier will exist for non-row ID columns
                constraintBuilder.add(new AccumuloColumnConstraint(col.getName(), col.getFamily().get(),
                        col.getQualifier().get(), Optional.of(cd.getDomain()), col.isIndexed()));
            }
        }

        return constraintBuilder.build();
    }
}
