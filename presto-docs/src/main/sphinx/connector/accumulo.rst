..
.. Licensed under the Apache License, Version 2.0 (the "License");
.. you may not use this file except in compliance with the License.
.. You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

Accumulo Connector
==================

The Accumulo connector supports reading and writing data from Apache Accumulo.
Please read this page thoroughly to understand the capabilities and features of the connector.

Table of Contents
~~~~~~~~~~~~~~~~~
#. `Dependencies <#dependencies>`__
#. `Installing the Iterator Dependency <#installing-the-iterator-dependency>`__
#. `Connector Configuration <#connector-configuration>`__
#. `Unsupported Features <#unsupported-features>`__
#. `Usage <#usage>`__
#. `Indexing Columns <#indexing-columns>`__
#. `Loading Data <#loading-data>`__
#. `External Tables <#external-tables>`__
#. `Table Properties <#table-properties>`__
#. `Session Properties <#session-properties>`__
#. `Adding Columns <#adding-columns>`__
#. `Serializers <#serializers>`__
#. `Metadata Management <#metadata-management>`__

Dependencies
~~~~~~~~~~~~
-  Java 1.8 (required for connector)
-  Java 1.7 (``accumulo.jdk.version == 1.7 ? required : !required``)
-  Maven
-  Accumulo
-  *presto-accumulo-iterators*, from `https://github.com/bloomberg/presto-accumulo <https://github.com/bloomberg/presto-accumulo>`_

Installing the Iterator Dependency
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The Accumulo connector uses custom Accumulo iterators in
order to push various information in a SQL predicate clause to Accumulo for
server-side filtering, known as *predicate pushdown*. In order
for the server-side iterators to work, you need to add the ``presto-accumulo``
jar file to Accumulo's ``lib/ext`` directory on each TabletServer node.

.. code:: bash

    # For each TabletServer node:
    scp $PRESTO_HOME/plugins/accumulo/presto-accumulo-*.jar [tabletserver_address]:$ACCUMULO_HOME/lib/ext

    # TabletServer should pick up new JAR files in ext directory, but may require restart

Note that this uses Java 8.  If your Accumulo cluster is using Java 7,
you'll receive an ``Unsupported major.minor version 52.0`` error in your TabletServer logs when you
attempt to create an indexed table.  You'll instead need to use the *presto-accumulo-iterators* jar file
that is located at `https://github.com/bloomberg/presto-accumulo <https://github.com/bloomberg/presto-accumulo>`_.

Connector Configuration
~~~~~~~~~~~~~~~~~~~~~~~
See ``presto-accumulo/etc/catalog/accumulo.properties`` for an example
configuration of the Accumulo connector. Fill in the appropriate values
to fit your cluster, then copy this file into
``$PRESTO_HOME/etc/catalog``. The list of configuration variables is
below.

Restart the Presto server, then use the presto client with the
``accumulo`` catalog like so:

.. code:: bash

    presto --server localhost:8080 --catalog accumulo --schema default

Configuration Variables
-----------------------
+----------------------------------+------------------+----------+--------------------------------------------------------------------------------------------------+
| Parameter Name                   | Default Value    | Required | Description                                                                                      |
+==================================+==================+==========+==================================================================================================+
| connector.name                   | (none)           | Yes      | Name of the connector. Do not change!                                                            |
+----------------------------------+------------------+----------+--------------------------------------------------------------------------------------------------+
| instance                         | (none)           | Yes      | Name of the Accumulo instance                                                                    |
+----------------------------------+------------------+----------+--------------------------------------------------------------------------------------------------+
| zookeepers                       | (none)           | Yes      | ZooKeeper connect string                                                                         |
+----------------------------------+------------------+----------+--------------------------------------------------------------------------------------------------+
| username                         | (none)           | Yes      | Accumulo user for Presto                                                                         |
+----------------------------------+------------------+----------+--------------------------------------------------------------------------------------------------+
| password                         | (none)           | Yes      | Accumulo password for user                                                                       |
+----------------------------------+------------------+----------+--------------------------------------------------------------------------------------------------+
| zookeeper.metadata.root          | /presto-accumulo | No       | Root znode for storing metadata. Only relevant if using default Metadata Manager                 |
+----------------------------------+------------------+----------+--------------------------------------------------------------------------------------------------+
| metadata.manager.class           | default          | No       | Fully qualified classname for the MetadataManager class. Default is the ZooKeeperMetadataManager |
+----------------------------------+------------------+----------+--------------------------------------------------------------------------------------------------+
| cardinality.cache.size           | 100000           | No       | Gets the size of the index cardinality cache                                                     |
+----------------------------------+------------------+----------+--------------------------------------------------------------------------------------------------+
| cardinality.cache.expire.seconds | 300              | No       | Gets the expiration, in seconds, of the cardinality cache                                        |
+----------------------------------+------------------+----------+--------------------------------------------------------------------------------------------------+

Unsupported Features
~~~~~~~~~~~~~~~~~~~~

Of the available Presto DDL/DML statements and features, the Accumulo connector does **not** support:

- **Adding columns via ALTER TABLE**: While you cannot add columns via SQL, you can using a tool.
  See the below section on `Adding Columns <#adding-columns>`__ for more.
- **DELETE**: Deleting rows are not yet implemented for the connector, but you could always use the Accumulo API
- **Transactions** : Transaction support has not yet been implemented for the connector

Usage
~~~~~

Simply begin using SQL to create a new table in Accumulo to begin
working with data. By default, the first column of the table definition
is set to the Accumulo row ID. This should be the primary key of your
table, and keep in mind that any ``INSERT statements`` containing the same
row ID is effectively an UPDATE as far as Accumulo is concerned, as any
previous data in the cell will be overwritten. The row ID can be
any valid Presto datatype. If the first column is not your primary key, you
can set the row ID column using the ``row_id`` table property within the ``WITH``
clause of your table definition.

Simply issue a ``CREATE TABLE`` statement to create a new Presto/Accumulo table.

.. code:: sql

    CREATE TABLE myschema.scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE);

    DESCRIBE myschema.scientists;
      Column   |  Type   |                  Comment
    -----------+---------+-------------------------------------------
     recordkey | varchar | Accumulo row ID
     name      | varchar | Accumulo column f9eb:0905. Indexed: false
     age       | bigint  | Accumulo column 297d:b43b. Indexed: false
     birthday  | date    | Accumulo column 0bc4:bb7c. Indexed: false

This command will create a new Accumulo table with the ``recordkey`` column
as the Accumulo row ID. The name, age, and birthday columns are mapped to
auto-generated column family and qualifier values.

When creating a table using SQL, you can optionally specify a
``column_mapping`` table property. The value of this property is a
comma-delimited list of triples, presto column **:** accumulo column
family **:** accumulo column qualifier, with one triple for every
non-row ID column. This sets the mapping of the Presto column name to
the corresponding Accumulo column family and column qualifier.

If you don't specify the ``column_mapping`` table property, then the
connector will auto-generate column names (respecting any configured locality groups).
Auto-generation of column names is only available for internal tables, so if your
table is external you must specify the column_mapping property.

For a full list of table properties, see `Table Properties <#table-properties>`__.

For example:

.. code:: sql

    CREATE TABLE myschema.scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE)
    WITH (
      column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date');

    DESCRIBE myschema.scientists;
      Column   |  Type   |                    Comment
    -----------+---------+-----------------------------------------------
     recordkey | varchar | Accumulo row ID
     name      | varchar | Accumulo column metadata:name. Indexed: false
     age       | bigint  | Accumulo column metadata:age. Indexed: false
     birthday  | date    | Accumulo column metadata:date. Indexed: false

You can then issue INSERT statements to put data into Accumulo.

**WARNING**: While issuing ``INSERT`` statements sure is convenient,
this method of loading data into Accumulo is low-throughput. You'll want
to use the Accumulo APIs to write ``Mutations`` directly to the tables.
See the section on `Loading Data <#loading-data>`__ for more details.

.. code:: sql

    INSERT INTO myschema.scientists VALUES
    ('row1', 'Grace Hopper', 109, DATE '1906-12-09' ),
    ('row2', 'Alan Turing', 103, DATE '1912-06-23' );

    SELECT * FROM myschema.scientists;
     recordkey |     name     | age |  birthday  
    -----------+--------------+-----+------------
     row1      | Grace Hopper | 109 | 1906-12-09 
     row2      | Alan Turing  | 103 | 1912-06-23 
    (2 rows)

As you'd expect, rows inserted into Accumulo via the shell or
programatically will also show up when queried. (The Accumulo shell
thinks "-5321" is an option and not a number... so we'll just make TBL a
little younger.)

::

    $ accumulo shell -u root -p secret
    root@default> table myschema.scientists
    root@default myschema.scientists> insert row3 metadata name "Tim Berners-Lee"
    root@default myschema.scientists> insert row3 metadata age 60
    root@default myschema.scientists> insert row3 metadata date 5321

.. code:: sql

    presto:default> SELECT * FROM myschema.scientists;
     recordkey |      name       | age |  birthday  
    -----------+-----------------+-----+------------
     row1      | Grace Hopper    | 109 | 1906-12-09 
     row2      | Alan Turing     | 103 | 1912-06-23 
     row3      | Tim Berners-Lee |  60 | 1984-07-27 
    (3 rows)

You can also drop tables using the DROP command. This command drops both
metadata and the tables. See the below section on `External
Tables <#external-tables>`__ for more details on internal and external
tables.

.. code:: sql

    DROP TABLE myschema.scientists;

Indexing Columns
~~~~~~~~~~~~~~~~~~

Internally, the connector creates an Accumulo ``Range`` and packs it in
a split. This split gets passed to a Presto Worker to read the data from
the ``Range`` via a ``BatchScanner``. When issuing a query that results
in a full table scan, each Presto Worker gets a single ``Range`` that
maps to a single tablet of the table. When issuing a query with a
predicate (i.e. ``WHERE x = 10`` clause), Presto passes the values
within the predicate (``10``) to the connector so it can use this
information to scan less data. When the Accumulo row ID is used as part
of the predicate clause, this narrows down the ``Range`` lookup to quickly
retrieve a subset of data from Accumulo.

But what about the other columns? If you're frequently querying on
non-row ID columns, you should consider using the **indexing**
feature built into the Accumulo connector. This feature can drastically
reduce query runtime when selecting a handful of values from the table,
and the heavy lifting is done for you when loading data via Presto
``INSERT`` statements (though, keep in mind writing data to Accumulo via
``INSERT`` does not have high throughput).

To enable indexing, add the ``index_columns`` table property and specify
a comma-delimited list of Presto column names you wish to index (we use the
``string`` serializer here to help with this example -- you
should be using the default ``lexicoder`` serializer).

.. code:: sql

    presto:default> CREATE TABLE myschema.scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE)
    WITH (
      serializer = 'string',
      index_columns='name,age,birthday'
    );

After creating the table, we see there are an additional two Accumulo
tables to store the index and metrics.

::

    root@default> tables
    accumulo.metadata
    accumulo.root
    myschema.scientists
    myschema.scientists_idx
    myschema.scientists_idx_metrics
    trace

After inserting data, we can look at the index table and see there are
indexed values for the name, age, and birthday columns. The connector
queries this index table

.. code:: sql

    presto:default> INSERT INTO myschema.scientists VALUES
    ('row1', 'Grace Hopper', 109, DATE '1906-12-09' ),
    ('row2', 'Alan Turing', 103, DATE '1912-06-23' );

    root@default> scan -t myschema.scientists_idx
    -21011 metadata_date:row2 []
    -23034 metadata_date:row1 []
    103 metadata_age:row2 []
    109 metadata_age:row1 []
    Alan Turing metadata_name:row2 []
    Grace Hopper metadata_name:row1 []

When issuing a query with a ``WHERE`` clause against indexed columns,
the connector searches the index table for all row IDs that contain the
value within the predicate. These row IDs are bundled into a Presto
split as single-value ``Range`` objects (the number of row IDs per split
is controlled by the value of ``accumulo.index_rows_per_split``) and
passed to a Presto worker to be configured in the ``BatchScanner`` which
scans the data table.

.. code:: sql

    presto:default> SELECT * FROM myschema.scientists WHERE age = 109;
     recordkey |     name     | age |  birthday
    -----------+--------------+-----+------------
     row1      | Grace Hopper | 109 | 1906-12-09
    (1 row)

Loading Data
~~~~~~~~~~~~
The Accumulo connector supports loading data via INSERT statements, however
this method tends to be low-throughput and should not be relied on when throughput
is a concern. Instead, users of the connector should use the ``PrestoBatchWriter``
tool that is provided as part of the presto-accumulo-tools subproject in the
`presto-accumulo repository <https://github.com/bloomberg/presto-accumulo>`_.

The ``PrestoBatchWriter`` is a wrapper class for the typical ``BatchWriter`` that
leverages the Presto/Accumulo metadata to write Mutations to the main data table.
In particular, it handles indexing the given mutations on any indexed columns.
Usage of the tool is provided in the README in the `repository <https://github.com/bloomberg/presto-accumulo>`_.

External Tables
~~~~~~~~~~~~~~~

By default, the tables created using SQL statements via Presto are
*internal* tables, that is both the Presto table metadata and the
Accumulo tables are managed by Presto. When you create an internal
table, the Accumulo table is created as well. You will receive an error
if the Accumulo table already exists. When an internal table is dropped
via Presto, the Accumulo table (and any index tables) are dropped as
well.

To change this behavior, set the ``external`` property to ``true`` when
issuing the ``CREATE`` statement. This will make the table an *external*
table, and a ``DROP TABLE`` command will **only** delete the metadata
associated with the table -- the Accumulo tables and data remain
untouched.

External tables can be a bit more difficult to work with, as the data is stored
in an expected format. If the data is not stored correctly, then you're
gonna have a bad time. Users must provide a ``column_mapping`` property
when creating the table. This creates the mapping of Presto column name
to the column family/qualifier for the cell of the table. The value of the
cell is stored in the ``Value`` of the Accumulo key/value pair. By default,
this value is expected to be serialized using Accumulo's *lexicoder* API.
If you are storing values as strings, you can specify a different serializer
using the ``serializer`` property of the table. See the section on
`Table Properties <#table-properties>`__ for more information.

Note that the Accumulo table and any index tables (if applicable) must
exist prior to creating the external table. First, we create an Accumulo
table called ``external_table``.

.. code:: sql

    root@default> createtable external_table

Next, we create the Presto external table.

.. code:: sql

    presto:default> CREATE TABLE external_table (a VARCHAR, b BIGINT, c DATE) 
    WITH (
        column_mapping = 'a:md:a,b:md:b,c:md:c',
        external = true
    );

After creating the table, usage of the table continues as usual:

.. code:: sql

    presto:default> INSERT INTO external_table VALUES ('1', 1, DATE '2015-03-06'), ('2', 2, DATE '2015-03-07');
    INSERT: 2 rows

    presto:default> SELECT * FROM external_table;
     a | b |     c      
    ---+---+------------
     1 | 1 | 2015-03-06 
     2 | 2 | 2015-03-06 
    (2 rows)

    presto:default> DROP TABLE external_table;
    DROP TABLE

After dropping the table, the table will still exist in Accumulo because
it is *external*.

.. code:: sql

    root@default> tables
    accumulo.metadata
    accumulo.root
    external_table
    trace

Table Properties
~~~~~~~~~~~~~~~~~~

Table property usage example:

.. code:: sql

    CREATE TABLE myschema.scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE)
    WITH (
      column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
      index_columns = 'name,age'
    );

+-----------------+----------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Property Name   | Default Value  | Description                                                                                                                                                                                                                                                                        |
+=================+================+====================================================================================================================================================================================================================================================================================+
| column_mapping  | (generated     | Comma-delimited list of column metadata: col_name:col_family:col_qualifier,[...]. Required for external tables.  Not setting this property results in auto-generated column names.                                                                                                 |
+-----------------+----------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| index_columns   | (none)         | A comma-delimited list of Presto columns that are indexed in this table's corresponding index table                                                                                                                                                                                |
+-----------------+----------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| external        | false          | If true, Presto will only do metadata operations for the table. Else, Presto will create and drop Accumulo tables where appropriate.                                                                                                                                               |
+-----------------+----------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| locality_groups | (none)         | List of locality groups to set on the Accumulo table. Only valid on internal tables. String format is locality group name, colon, comma delimited list of column families in the group. Groups are delimited by pipes. Example: group1:famA,famB,famC|group2:famD,famE,famF|etc... |
+-----------------+----------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| row_id          | (first column) | Presto column name that maps to the Accumulo row ID.                                                                                                                                                                                                                               |
+-----------------+----------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| serializer      | default        | Serializer for Accumulo data encodings. Can either be 'default', 'string', 'lexicoder', or a Java class name. Default is 'default', i.e. the value from AccumuloRowSerializer.getDefault(), i.e. 'lexicoder'.                                                                      |
+-----------------+----------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| scan_auths      | (user auths)   | Scan-time authorizations set on the batch scanner.                                                                                                                                                                                                                                 |
+-----------------+----------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Session Properties
~~~~~~~~~~~~~~~~~~

You can change the default value of a session property by using the SET
SESSION clause in the Presto CLI or at the top of your Presto script:

.. code:: sql

    SET SESSION accumulo.column_filter_optimizations_enabled = false;

+---------------------------------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Property Name                               | Default Value | Description                                                                                                                                                           |
+=============================================+===============+=======================================================================================================================================================================+
| accumulo.optimize_column_filter_pushdowns   | false         | Experimental. Set to true to enable the column value filter pushdowns                                                                                                 |
+---------------------------------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| accumulo.optimize_locality_enabled          | true          | Set to true to enable data locality for non-indexed scans                                                                                                             |
+---------------------------------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| accumulo.optimize_split_ranges_enabled      | true          | Set to true to split non-indexed queries by tablet splits. Should generally be true.                                                                                  |
+---------------------------------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| accumulo.optimize_index_enabled             | true          | Set to true to enable usage of the secondary index on query                                                                                                           |
+---------------------------------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| accumulo.index_rows_per_split               | 10000         | The number of Accumulo row IDs that are packed into a single Presto split                                                                                             |
+---------------------------------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| accumulo.index_threshold                    | 0.2           | The ratio between number of rows to be scanned based on the index over the total number of rows. If the ratio is below this threshold, the index will be used.        |
+---------------------------------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| accumulo.index_lowest_cardinality_threshold | 0.01          | The threshold where the column with the lowest cardinality will be used instead of computing an intersection of ranges in the index. Secondary index must be enabled. |
+---------------------------------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| accumulo.index_metrics_enabled              | true          | Set to true to enable usage of the metrics table to optimize usage of the index                                                                                       |
+---------------------------------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| accumulo.scan_username                      | (config)      | User to impersonate when scanning the tables. This property trumps the scan_auths table property. Default is the user in the connector configuration file.            |
+---------------------------------------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Adding Columns
~~~~~~~~~~~~~~

Adding a new column to an existing table cannot be done today via
``ALTER TABLE [table] ADD COLUMN [name] [type]`` because of the additional
metadata required for the columns to work; the column family, qualifier,
and if the column is indexed.

Instead, you can use one of the utilities in the
`presto-accumulo-tools <https://github.com/bloomberg/presto-accumulo/tree/master/presto-accumulo-tools>`__
sub-project of the ``presto-accumulo`` repository.  Documentation and usage can be found in the README.

Serializers
~~~~~~~~~~~

The Presto connector for Accumulo has a pluggable serializer framework
for handling I/O between Presto and Accumulo. This enables end-users the
ability to programatically serialized and deserialize their special data
formats within Accumulo, while abstracting away the complexity of the
connector itself.

There are two types of serializers currently available; a ``string``
serializer that treats values as Java ``String`` and a ``lexicoder``
serializer that leverages Accumulo's Lexicoder API to store values. The
default serializer is the ``lexicoder`` serializer, as this serializer
does not require expensive conversion operations back and forth between
``String`` objects and the Presto types -- the cell's value is encoded as a
byte array.

You can change the default the serializer by specifying the
``serializer`` table property, using either ``default`` (which is
``lexicoder``), ``string`` or ``lexicoder`` for the built-in types, or
you could provide your own implementation by extending
``AccumuloRowSerializer``, adding it to the Presto ``CLASSPATH``, and
specifying the fully-qualified Java class name in the connector configuration.

.. code:: sql

    presto:default> CREATE TABLE myschema.scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
    WITH (
        column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
        serializer = 'default'
    );

    presto:default> INSERT INTO myschema.scientists VALUES
    ('row1', 'Grace Hopper', 109, DATE '1906-12-09' ),
    ('row2', 'Alan Turing', 103, DATE '1912-06-23' );

.. code:: bash

    root@default> scan -t myschema.scientists
    row1 metadata:age []    \x08\x80\x00\x00\x00\x00\x00\x00m
    row1 metadata:date []    \x08\x7F\xFF\xFF\xFF\xFF\xFF\xA6\x06
    row1 metadata:name []    Grace Hopper
    row2 metadata:age []    \x08\x80\x00\x00\x00\x00\x00\x00g
    row2 metadata:date []    \x08\x7F\xFF\xFF\xFF\xFF\xFF\xAD\xED
    row2 metadata:name []    Alan Turing

.. code:: sql

    presto:default> CREATE TABLE myschema.stringy_scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
    WITH (
        column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
        serializer = 'string'
    );

    presto:default> INSERT INTO myschema.stringy_scientists VALUES
    ('row1', 'Grace Hopper', 109, DATE '1906-12-09' ),
    ('row2', 'Alan Turing', 103, DATE '1912-06-23' );

.. code:: bash

    root@default> scan -t myschema.stringy_scientists
    row1 metadata:age []    109
    row1 metadata:date []    -23034
    row1 metadata:name []    Grace Hopper
    row2 metadata:age []    103
    row2 metadata:date []    -21011
    row2 metadata:name []    Alan Turing

.. code:: sql

    CREATE TABLE myschema.custom_scientists (recordkey VARCHAR, name VARCHAR, age BIGINT, birthday DATE) 
    WITH (
        column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
        serializer = 'my.serializer.package.MySerializer'
    );

Metadata Management
~~~~~~~~~~~~~~~~~~~

Metadata management for the Accumulo tables is pluggable, with an
initial implementation storing the data in ZooKeeper. You can (and
should) issue SQL statements in Presto to create and drop tables. This
is the easiest method of creating the metadata required to make the
connector work. It is best to not mess with the metadata, but here are
the details of how it is stored. Information is power.

A root node in ZooKeeper holds all the mappings, and the format is as
follows:

.. code:: bash

    /metadata-root/schema/table

Where ``metadata-root`` is the value of ``zookeeper.metadata.root`` in
the config file (default is ``/presto-accumulo``), ``schema`` is the
Presto schema (which is identical to the Accumulo namespace name), and
``table`` is the Presto table name (again, identical to Accumulo name).
The data of the ``table`` ZooKeeper node is a serialized
``AccumuloTable`` Java object (which resides in the connector code).
This table contains the schema (namespace) name, table name, column
definitions, the serializer to use for the table, and any additional
table properties.

If you have a need to programmatically manipulate the ZooKeeper metadata
for Accumulo, take a look at
``com.facebook.presto.accumulo.metadata.ZooKeeperMetadataManager`` for some
Java code to simplify the process.
