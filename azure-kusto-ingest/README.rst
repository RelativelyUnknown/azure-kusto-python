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
Arrow data (a ``pyarrow.Table``, ``RecordBatch`` or ``RecordBatchReader``, or any object that implements the Arrow PyCapsule stream interface)
can be ingested as well, after installing the package with the "pyarrow" extra (``pip install azure-kusto-ingest[pyarrow]``).
It is written to a Parquet file, so the ingestion properties may have the Parquet format:

.. code-block:: python

    import pyarrow

    table = pyarrow.table({"id": [1, 2], "name": ["abc", "cde"]})
    ingestion_props = IngestionProperties(database="{database_name}", table="{table_name}", data_format=DataFormat.PARQUET)
    client.ingest_from_arrow(table, ingestion_properties=ingestion_props)

Kusto can't ingest a timespan from a Parquet file, so ``duration`` columns are rejected.

Timestamps are written with microsecond resolution, which every Parquet reader supports (Spark, Databricks and Fabric can't read nanosecond timestamps).
A ``timestamp[ns]`` column is truncated to microseconds, so the last digit of a Kusto datetime's fraction of a second is dropped.
To write the nanoseconds as they are, pass ``parquet_options={"coerce_timestamps": None}``, if the cluster accepts them.

To keep every 100 nanosecond tick of a datetime, ingest the column as integer nanoseconds and let the mapping convert it. The queued client takes the mapping
inline; the streaming and managed streaming clients need a mapping which was created in advance, passed as ``ingestion_mapping_reference``:

.. code-block:: python

    table = pyarrow.table({"id": [1], "when": pyarrow.array([1_700_000_000_123_456_700], pyarrow.timestamp("ns", "UTC"))})
    table = table.set_column(1, "when", table.column("when").cast(pyarrow.int64()))  # nanoseconds since 1970
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

Without the mapping, an integer column that is ingested to a datetime column gives empty or wrong values.

Links
~~~~~

* `How to install the package <https://github.com/Azure/azure-kusto-python#install>`_.

* `Data ingest sample <https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-ingest/tests/sample.py>`_.

* `GitHub Repository <https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-data>`_.
