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

import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.QueryAssertions;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.testng.annotations.Test;

import java.nio.charset.Charset;

import static com.facebook.presto.accumulo.AccumuloQueryRunner.createAccumuloQueryRunner;

public class TestAccumuloAnyQueries
        extends AbstractTestQueryFramework
{
    public TestAccumuloAnyQueries()
            throws Exception
    {
        super(createAccumuloQueryRunner(ImmutableMap.of(), false));
    }

    @Test
    public void testBigint()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b bigint) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, 123), (2, 456)");
            computeActual("SELECT * FROM test WHERE b ANY ARRAY[BIGINT '123', BIGINT '456']");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testBigintArray()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b array(bigint)) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, ARRAY[BIGINT '123', BIGINT '456']), (2, ARRAY[BIGINT '456', BIGINT '789'])");
            computeActual("SELECT * FROM test WHERE BIGINT '456' ANY b");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testBoolean()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b boolean) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, true), (2, false)");
            computeActual("SELECT * FROM test WHERE b ANY ARRAY[true, false]");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testBooleanArray()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b array(boolean)) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, ARRAY[true, false]), (2, ARRAY[true, false])");
            computeActual("SELECT * FROM test WHERE true ANY b");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testInteger()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b integer) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, 123), (2, 456)");
            computeActual("SELECT * FROM test WHERE b ANY ARRAY[123, 456]");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testIntegerArray()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b array(integer)) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, ARRAY[123, 456]), (2, ARRAY[456, 789])");
            computeActual("SELECT * FROM test WHERE 456 ANY b");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testDate()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b date) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, DATE '2016-05-25'), (2, DATE '2016-05-26')");
            computeActual("SELECT * FROM test WHERE b ANY ARRAY[DATE '2016-05-25', DATE '2016-05-26']");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testDateArray()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b array(date)) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, ARRAY[DATE '2016-05-25', DATE '2016-05-26']), (2, ARRAY[DATE '2016-05-26', DATE '2016-05-27'])");
            computeActual("SELECT * FROM test WHERE DATE '2016-05-25' ANY b");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testDouble()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b double) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, 123.0), (2, 456.0)");
            computeActual("SELECT * FROM test WHERE b ANY ARRAY[123.0, 456.0]");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testDoubleArray()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b array(double)) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, ARRAY[123.0, 456.0]), (2, ARRAY[456.0, 789.0])");
            computeActual("SELECT * FROM test WHERE 456.0 ANY b");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testTimestamp()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b timestamp) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, TIMESTAMP '2016-05-26 12:00:00'), (2, TIMESTAMP '2016-05-27 12:00:00')");
            computeActual("SELECT * FROM test WHERE b ANY ARRAY[TIMESTAMP '2016-05-26 12:00:00', TIMESTAMP '2016-05-27 12:00:00']");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testTimestampArray()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b array(timestamp)) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, ARRAY[TIMESTAMP '2016-05-26 12:00:00', TIMESTAMP '2016-05-27 12:00:00']), (2, ARRAY[TIMESTAMP '2016-05-27 12:00:00', TIMESTAMP '2016-05-28 12:00:00'])");
            computeActual("SELECT * FROM test WHERE TIMESTAMP '2016-05-27 12:00:00' ANY b");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testTime()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b time) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, TIME '01:02:03.123'), (2, TIME '04:05:06.456')");
            computeActual("SELECT * FROM test WHERE b ANY ARRAY[TIME '01:02:03.123', TIME '04:05:06.456']");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testTimeArray()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b array(time)) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, ARRAY[TIME '01:02:03.123', TIME '04:05:06.456']), (2, ARRAY[TIME '04:05:06.456', TIME '07:08:09.789'])");
            computeActual("SELECT * FROM test WHERE TIME '04:05:06.456' ANY b");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testVarbinary()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b varbinary) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, VARBINARY '123'), (2, VARBINARY '456')");
            computeActual("SELECT * FROM test WHERE b ANY ARRAY[VARBINARY '123', VARBINARY '456']");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testVarbinaryArray()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b array(varbinary)) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, ARRAY[VARBINARY '123', VARBINARY '456']), (2, ARRAY[VARBINARY '456', VARBINARY '789'])");
            computeActual("SELECT * FROM test WHERE VARBINARY '456' ANY b");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testVarchar()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b varchar) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, '123'), (2, '456')");
            computeActual("SELECT * FROM test WHERE b ANY ARRAY['123', '456']");
        }
        finally {
            dropTable("test");
        }
    }

    @Test
    public void testVarcharArray()
            throws Exception
    {
        try {
            computeActual("CREATE TABLE test (a integer, b array(varchar)) WITH (index_columns='b')");
            computeActual("INSERT INTO test VALUES (1, ARRAY['123', '456']), (2, ARRAY['456', '789'])");
            computeActual("SELECT * FROM test WHERE '456' ANY b");
        }
        finally {
            dropTable("test");
        }
    }

    private void dropTable(String table)
    {
        try {
            assertUpdate("DROP TABLE " + table);
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "Failed to drop table " + table);
        }
    }
}
