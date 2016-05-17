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
import com.facebook.presto.accumulo.Types;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.index.Indexer;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.Field;
import com.facebook.presto.accumulo.model.Row;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeUtils;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.accumulo.AccumuloErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.accumulo.AccumuloErrorCode.VALIDATION;
import static java.util.Objects.requireNonNull;

/**
 * Output class for serializing Presto pages (blocks of rows of data) to Accumulo. This class
 * converts the rows from within a page to a collection of Accumulo Mutations, writing and indexed
 * the rows. Writers are flushed and closed on commit, and if a rollback occurs... we'll you're
 * gonna have a bad time.
 *
 * @see AccumuloPageSinkProvider
 */
public class AccumuloPageSink
        implements ConnectorPageSink
{
    private final AccumuloRowSerializer serializer;
    private final BatchWriter wrtr;
    private final Indexer indexer;
    private final List<AccumuloColumnHandle> columns;
    private Integer rowIdOrdinal;

    /**
     * Creates a new instance of {@link AccumuloPageSink}
     *
     * @param config Configuration for Accumulo
     * @param table Table metadata for an Accumulo table
     */
    public AccumuloPageSink(AccumuloConfig config, AccumuloTable table)
    {
        requireNonNull(config, "config is null");
        requireNonNull(table, "table is null");

        this.columns = table.getColumns();

        // Fetch the row ID ordinal, throwing an exception if not found for safety
        for (AccumuloColumnHandle ach : columns) {
            if (ach.getName().equals(table.getRowId())) {
                rowIdOrdinal = ach.getOrdinal();
                break;
            }
        }

        if (rowIdOrdinal == null) {
            throw new PrestoException(INTERNAL_ERROR, "Row ID ordinal not found");
        }

        this.serializer = table.getSerializerInstance();

        try {
            Connector conn = AccumuloClient.getAccumuloConnector(config);
            // Create a BatchWriter to the Accumulo table
            BatchWriterConfig conf = new BatchWriterConfig();
            wrtr = conn.createBatchWriter(table.getFullTableName(), conf);

            // If the table is indexed, create an instance of an Indexer, else null
            if (table.isIndexed()) {
                indexer = new Indexer(conn,
                        conn.securityOperations().getUserAuthorizations(config.getUsername()),
                        table, conf);
            }
            else {
                indexer = null;
            }
        }
        catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR,
                    "Accumulo error when creating BatchWriter and/or Indexer", e);
        }
    }

    /**
     * Converts a {@link Row} to an Accumulo mutation.
     *
     * @param row Row object
     * @param rowIdOrdinal Ordinal in the list of columns that is the row ID. This isn't checked at all, so I
     * hope you're right. Also, it is expected that the list of column handles is sorted
     * in ordinal order. This is a very demanding function.
     * @param columns All column handles for the Row, sorted by ordinal.
     * @param serializer Instance of {@link AccumuloRowSerializer} used to encode the values of the row to
     * the Mutation
     * @return Mutation
     */
    public static Mutation toMutation(Row row, int rowIdOrdinal, List<AccumuloColumnHandle> columns,
            AccumuloRowSerializer serializer)
    {
        // Set our value to the row ID
        Text value = new Text();
        Field rField = row.getField(rowIdOrdinal);
        if (rField.isNull()) {
            throw new PrestoException(VALIDATION, "Column mapped as the Accumulo row ID cannot be null");
        }

        setText(rField, value, serializer);

        // Iterate through all the column handles, setting the Mutation's columns
        Mutation m = new Mutation(value);
        for (AccumuloColumnHandle ach : columns) {
            // Skip the row ID ordinal
            if (ach.getOrdinal() == rowIdOrdinal) {
                continue;
            }

            // If the value of the field is not null
            if (!row.getField(ach.getOrdinal()).isNull()) {
                // Serialize the value to the text
                setText(row.getField(ach.getOrdinal()), value, serializer);

                // And add the bytes to the Mutation
                m.put(ach.getFamily(), ach.getQualifier(), new Value(value.copyBytes()));
            }
        }

        return m;
    }

    /**
     * Sets the value of the given Text object to the encoded value of the given field.
     *
     * @param field Value of the field to encode
     * @param value Text object to set
     * @param serializer Serializer to use to encode the value
     */
    private static void setText(Field field, Text value, AccumuloRowSerializer serializer)
    {
        Type type = field.getType();
        if (Types.isArrayType(type)) {
            serializer.setArray(value, type, field.getArray());
        }
        else if (Types.isMapType(type)) {
            serializer.setMap(value, type, field.getMap());
        }
        else {
            if (type instanceof BigintType) {
                serializer.setLong(value, field.getBigInt());
            }
            else if (type instanceof BooleanType) {
                serializer.setBoolean(value, field.getBoolean());
            }
            else if (type instanceof DateType) {
                serializer.setDate(value, field.getDate());
            }
            else if (type instanceof DoubleType) {
                serializer.setDouble(value, field.getDouble());
            }
            else if (type instanceof IntegerType) {
                serializer.setInt(value, field.getInt());
            }
            else if (type instanceof TimeType) {
                serializer.setTime(value, field.getTime());
            }
            else if (type instanceof TimestampType) {
                serializer.setTimestamp(value, field.getTimestamp());
            }
            else if (type instanceof VarbinaryType) {
                serializer.setVarbinary(value, field.getVarbinary());
            }
            else if (type instanceof VarcharType) {
                serializer.setVarchar(value, field.getVarchar());
            }
            else {
                throw new UnsupportedOperationException("Unsupported type " + type);
            }
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page, Block sampleWeightBlock)
    {
        // For each position within the page, i.e. row
        for (int position = 0; position < page.getPositionCount(); ++position) {
            Row row = Row.newRow();
            // For each channel within the page, i.e. column
            for (int channel = 0; channel < page.getChannelCount(); ++channel) {
                // Get the type for this channel
                Type type = columns.get(channel).getType();

                // Read the value from the page and append the field to the row
                row.addField(TypeUtils.readNativeValue(type, page.getBlock(channel), position),
                        type);
            }

            // Convert row to a Mutation
            Mutation m = toMutation(row, rowIdOrdinal, columns, serializer);

            // If this mutation has columns
            if (m.size() > 0) {
                try {
                    // Write the mutation and index it
                    wrtr.addMutation(m);
                    if (indexer != null) {
                        indexer.index(m);
                    }
                }
                catch (MutationsRejectedException e) {
                    throw new PrestoException(INTERNAL_ERROR, "Mutation rejected by server", e);
                }
            }
            else {
                // Else, this Mutation contains only a row ID and will throw an exception if
                // added so, we throw one here with a more descriptive message!
                throw new PrestoException(VALIDATION,
                        "At least one non-recordkey column must contain a non-null value");
            }
        }

        return NOT_BLOCKED;
    }

    @Override
    public Collection<Slice> finish()
    {
        try {
            // Done serializing rows, so flush and close the writer and indexer
            wrtr.flush();
            wrtr.close();
            if (indexer != null) {
                indexer.close();
            }
        }
        catch (MutationsRejectedException e) {
            throw new PrestoException(INTERNAL_ERROR, "Mutation rejected by server on flush", e);
        }

        // TODO Look into any use of the metadata for writing out the rows
        return ImmutableList.of();
    }

    @Override
    public void abort()
    {
        // TODO Auto-generated method stub
    }
}
