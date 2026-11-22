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

    blob_descriptor = BlobDescriptor("https://{path_to_blob}.csv.gz?sas", 51200)  # in this example, the raw (uncompressed) size of the data is 50KB (51200 bytes)
    client.ingest_from_blob(blob_descriptor, ingestion_properties=ingestion_props)
    # Each call returns an IngestionResult; for queued ingestion its status is QUEUED, not "finished".


Overview
--------

*Kusto Python Ingest Client* is a library for ingesting data into Kusto clusters from Python.

It also works well from `Jupyter Notebooks <http://jupyter.org/>`_, including ones attached to Spark clusters, for instance on
`Azure Databricks <https://azure.microsoft.com/en-us/services/databricks>`_.

Choosing a client
~~~~~~~~~~~~~~~~~
* ``QueuedIngestClient`` uploads the data to storage and queues it for ingestion. It handles any size and is the right choice for most workloads, but the
  data shows up in the table after a short delay.
* ``KustoStreamingIngestClient`` sends the data directly to the engine, so it is available within seconds. It is meant for small amounts of data
  (up to 4 MB per call), and streaming ingestion has to be enabled on the cluster. Point it at the engine endpoint (``https://{cluster_name}.kusto.windows.net``).
* ``ManagedStreamingIngestClient`` tries streaming first, and falls back to queued ingestion when the data is too big or streaming keeps failing.

Ingesting from files, streams, blobs and pandas
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Besides the ``ingest_from_file`` and ``ingest_from_blob`` calls above, you can ingest from an open stream with ``ingest_from_stream``, and, with the "pandas"
extra (``pip install azure-kusto-ingest[pandas]``), from a ``DataFrame`` with ``ingest_from_dataframe``:

.. code-block:: python

    client.ingest_from_dataframe(df, ingestion_properties=ingestion_props)

Arrow
~~~~~
Arrow data (a ``pyarrow.Table``, ``RecordBatch`` or ``RecordBatchReader``, or any object that implements the Arrow PyCapsule stream interface)
can be ingested after installing the package with the "pyarrow" extra (``pip install azure-kusto-ingest[pyarrow]``).
It is written to a Parquet file, so the ingestion properties should use the Parquet format:

.. code-block:: python

    import pyarrow

    table = pyarrow.table({"id": [1, 2], "name": ["abc", "cde"]})
    ingestion_props = IngestionProperties(database="{database_name}", table="{table_name}", data_format=DataFormat.PARQUET)
    client.ingest_from_arrow(table, ingestion_properties=ingestion_props)

Two things to know:

* Kusto can't ingest a timespan from a Parquet file, so ``duration`` columns are rejected.
* Timestamps are written with microsecond resolution, which every Parquet reader supports. A ``timestamp[ns]`` column is truncated, so the last digit
  (100 ns) of a Kusto datetime's fraction of a second is dropped. If you need every tick, ingest the column as integer nanoseconds and let a mapping with
  ``TransformationMethod.DATE_TIME_FROM_UNIX_NANOSECONDS`` convert it. See the documentation of ``ingest_from_arrow`` for the details, including how to do that with
  the streaming clients.

Links
~~~~~

* `How to install the package <https://github.com/Azure/azure-kusto-python#install>`_.

* `Data ingest sample <https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-ingest/tests/sample.py>`_.

* `GitHub Repository <https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-ingest>`_.
