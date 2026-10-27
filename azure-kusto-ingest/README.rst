Microsoft Azure Kusto Ingest Library for Python
===============================================

.. code-block:: python

    from azure.kusto.data import KustoConnectionStringBuilder, DataFormat
    from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, FileDescriptor, BlobDescriptor

    ingestion_props = IngestionProperties(database="{database_name}", table="{table_name}", data_format=DataFormat.CSV)
    client = QueuedIngestClient(KustoConnectionStringBuilder.with_interactive_login("https://ingest-{cluster_name}.kusto.windows.net"))

    file_descriptor = FileDescriptor("{filename}.csv", 15360)  # in this example, the raw (uncompressed) size of the data is 15KB (15360 bytes)
    client.ingest_from_file(file_descriptor, ingestion_properties=ingestion_props)
    client.ingest_from_file("{filename}.csv", ingestion_properties=ingestion_props)

    blob_descriptor = BlobDescriptor("https://{path_to_blob}.csv.gz?sas", 51200)  # in this example, the raw (uncompressed) size of the data is 50KB (52100 bytes)
    client.ingest_from_blob(blob_descriptor, ingestion_properties=ingestion_props)


Overview
--------

*Kusto Python Ingest Client* Library provides the capability to ingest data into Kusto clusters using Python.
It is Python 3.x compatible and supports data types through familiar Python DB API interface.

It's possible to use the library, for instance, from `Jupyter Notebooks <http://jupyter.org/>`_ which are attached to Spark clusters,
including, but not exclusively, `Azure Databricks <https://azure.microsoft.com/en-us/services/databricks>`_ instances.

Arrow
~~~~~
Ingest Arrow data: a ``pyarrow.Table``, ``RecordBatch``, or ``RecordBatchReader``, or any object that implements the Arrow PyCapsule stream interface.
The client writes the data to a Parquet file and ingests the file. The examples use the ``client`` from the first example.

Install the package with the "pyarrow" extra:

.. code-block:: bash

    pip install azure-kusto-ingest[pyarrow]

Ingest an Arrow table
^^^^^^^^^^^^^^^^^^^^^
Use ``ingest_from_arrow`` to ingest an Arrow table:

.. code-block:: python

    import pyarrow

    table = pyarrow.table({"id": [1, 2], "name": ["abc", "cde"]})
    ingestion_props = IngestionProperties(database="{database_name}", table="{table_name}", data_format=DataFormat.PARQUET)
    client.ingest_from_arrow(table, ingestion_properties=ingestion_props)

Control the timestamp resolution
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default, ``ingest_from_arrow`` writes timestamps with microsecond resolution, which every Parquet reader supports.
Spark, Azure Databricks, and Microsoft Fabric can't read nanoseconds. A ``timestamp[ns]`` column is truncated to microseconds,
so the last digit of the fraction of a second is dropped.

Pass ``parquet_options`` to write nanoseconds as they are. The Kusto documentation doesn't say which timestamp units its Parquet reader accepts.

.. code-block:: python

    client.ingest_from_arrow(table, ingestion_properties=ingestion_props, parquet_options={"coerce_timestamps": None})

Ingest every tick of a datetime
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
A Kusto datetime counts ticks of 100 nanoseconds. To keep every tick, ingest the column as integer nanoseconds since 1970,
and use a Parquet mapping that converts it to a datetime.

First, cast the timestamp column to ``int64``:

.. code-block:: python

    table = pyarrow.table({"id": [1], "when": pyarrow.array([1_700_000_000_123_456_700], pyarrow.timestamp("ns", "UTC"))})
    table = table.set_column(1, "when", table.column("when").cast(pyarrow.int64()))

Then ingest the table with a mapping that uses the ``DateTimeFromUnixNanoseconds`` transformation:

.. code-block:: python

    from azure.kusto.ingest import ColumnMapping, TransformationMethod

    ingestion_props = IngestionProperties(
        database="{database_name}",
        table="{table_name}",
        data_format=DataFormat.PARQUET,
        column_mappings=[
            ColumnMapping(column_name="id", column_type="long", path="$.id"),
            ColumnMapping(column_name="when", column_type="datetime", path="$.when", transform=TransformationMethod.DATE_TIME_FROM_UNIX_NANOSECONDS),
        ],
    )
    client.ingest_from_arrow(table, ingestion_properties=ingestion_props)

Without the mapping, an integer column that goes into a datetime column is ingested as empty or wrong data.
The streaming and managed streaming clients don't accept a mapping in the request.
Create the mapping beforehand, and pass its name as ``ingestion_mapping_reference``.

Troubleshoot the ingestion of Arrow data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* ``KustoArrowNotInstalledError`` (an ``ImportError``): install pyarrow with ``pip install azure-kusto-ingest[pyarrow]``.
* ``ValueError`` about a duration column: Kusto can't ingest a timespan from a Parquet file.
  Cast the column to a string or to an integer, such as the number of ticks, and convert it after the ingestion.
* ``ValueError`` about a uuid column: pyarrow versions before 21 don't write the Parquet UUID type that Kusto needs for a guid. Upgrade pyarrow.
* ``ValueError`` about the ingestion mapping: Arrow data is ingested as Parquet.
  Create the ``IngestionProperties`` with ``data_format=DataFormat.PARQUET``.

Links
~~~~~

* `How to install the package <https://github.com/Azure/azure-kusto-python#install>`_.

* `Data ingest sample <https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-ingest/tests/sample.py>`_.

* `GitHub Repository <https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-data>`_.
