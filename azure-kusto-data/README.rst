Microsoft Azure Kusto Library for Python
========================================

Overview
--------

.. code-block:: python

    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

    cluster = "<insert here your cluster name>"
    client_id = "<insert here your AAD application id>"
    client_secret = "<insert here your AAD application key>"
    authority_id = "<insert here your AAD tenant id>"

    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret, authority_id)
    # It is a good practice to re-use the KustoClient instance, as it maintains a pool of connections to the Kusto service.
    # This sample shows how to create a client and close it in the same scope, for demonstration purposes.
    with KustoClient(kcsb) as client:
        db = "Samples"
        query = "StormEvents | take 10"

        response = client.execute(db, query)
        for row in response.primary_results[0]:
            print(row[0], " ", row["EventType"])



*Kusto Python Client* Library provides the capability to query Kusto clusters using Python.
It is Python 3.x compatible and supports
all data types through familiar Python DB API interface.

It's possible to use the library, for instance, from `Jupyter Notebooks
<http://jupyter.org/>`_.
which are attached to Spark clusters,
including, but not exclusively, `Azure Databricks
<https://azure.microsoft.com/en-us/services/databricks/>`_. instances.

Async Client
~~~~~~~~~~~~
Kusto now provides an asynchronous client for queries.

To use the client, first install the package with the "aio" extra:

.. code:: bash

    pip install azure-kusto-data[aio]

The async client uses exact same interface as the regular client, except
that it lives in the ``azure.kusto.data.aio`` namespace, and it returns
``Futures`` you will need to ``await`` its

.. code:: python

    from azure.kusto.data import KustoConnectionStringBuilder
    from azure.kusto.data.aio import KustoClient

    cluster = "<insert here your cluster name>"
    client_id = "<insert here your AAD application id>"
    client_secret = "<insert here your AAD application key>"
    authority_id = "<insert here your AAD tenant id>"


    async def sample():
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret, authority_id)
        async with KustoClient(kcsb) as client:
            db = "Samples"
            query = "StormEvents | take 10"

            response = await client.execute(db, query)
            for row in response.primary_results[0]:
                print(row[0], " ", row["EventType"])

Arrow
~~~~~
Convert query results to `Apache Arrow <https://arrow.apache.org/docs/python/>`_ tables and record batches. All Kusto data types are supported.
The examples use the ``client`` and ``db`` from the first example.

Install the package with the "pyarrow" extra:

.. code:: bash

    pip install azure-kusto-data[pyarrow]

Convert a result table
^^^^^^^^^^^^^^^^^^^^^^
Use ``arrow_table_from_result_table`` to convert a result table to a ``pyarrow.Table``:

.. code:: python

    from azure.kusto.data.arrow_helpers import arrow_table_from_result_table

    response = client.execute(db, "StormEvents | take 10")
    table = arrow_table_from_result_table(response.primary_results[0])

Convert all the primary results
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Use ``arrow_tables_from_response`` to convert all the primary results of a response to a list of tables:

.. code:: python

    from azure.kusto.data.arrow_helpers import arrow_tables_from_response

    response = client.execute(db, "StormEvents | take 10; StormEvents | count")
    tables = arrow_tables_from_response(response)

Stream a large result
^^^^^^^^^^^^^^^^^^^^^
Use ``arrow_reader_from_result_table`` to convert a streaming result batch by batch, without loading it into memory.
It returns a ``pyarrow.RecordBatchReader``:

.. code:: python

    from azure.kusto.data.arrow_helpers import arrow_reader_from_result_table

    streaming_response = client.execute_streaming_query(db, "StormEvents")
    reader = arrow_reader_from_result_table(next(streaming_response.iter_primary_results()))

Stream a large result with the async client
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Install the package with both extras, ``pip install azure-kusto-data[pyarrow,aio]``.
Then use ``arrow_record_batches_from_streaming_result_table`` to read the batches of a streaming result:

.. code:: python

    from azure.kusto.data.aio.arrow_helpers import arrow_record_batches_from_streaming_result_table

    streaming_response = await client.execute_streaming_query(db, "StormEvents")
    async for result in streaming_response.iter_primary_results():
        async for batch in arrow_record_batches_from_streaming_result_table(result):
            print(batch.num_rows)

Choose the time resolution
^^^^^^^^^^^^^^^^^^^^^^^^^^
Kusto counts time in ticks of 100 nanoseconds, and a datetime covers the years 0001-9999.
By default, datetimes and timespans become Arrow microsecond types, ``timestamp[us, UTC]`` and ``duration[us]``.
Most data tools expect microseconds, and microseconds cover the whole range. The conversion drops the last digit of the fraction of a second.

Pass ``time_unit="ns"`` to keep every tick:

.. code:: python

    table = arrow_table_from_result_table(response.primary_results[0], time_unit="ns")

Arrow nanoseconds only cover the years 1677-2262, so a datetime outside this range, such as ``9999-12-31``, becomes null.
Spark, Azure Databricks, and Microsoft Fabric can't read Parquet files with nanosecond timestamps.

Use the results with other tools
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The results were checked with pyarrow 26, pandas 2.3.1 and 3.0.5, Polars 2.1.0, DuckDB 2.0.0, and PySpark 4.2.0 (local mode):

.. list-table::
   :header-rows: 1

   * - Tool
     - ``time_unit="us"`` (the default)
     - ``time_unit="ns"``
   * - pandas
     - ``datetime64[us, UTC]`` and ``timedelta64[us]``, with the years 0001 and 9999 intact
     - ``datetime64[ns, UTC]`` and ``timedelta64[ns]``. A datetime outside 1677-2262 is ``NaT``.
   * - Polars
     - ``Datetime("us", "UTC")`` and ``Duration("us")``
     - The nanosecond variants
   * - DuckDB
     - ``TIMESTAMP WITH TIME ZONE`` and ``INTERVAL``
     - ``TIMESTAMPTZ_NS``, which keeps the seventh digit. ``INTERVAL`` has microsecond resolution.
   * - Spark (Parquet file)
     - Reads as ``timestamp``, from ``0001-01-01`` to ``9999-12-31``
     - Fails with ``PARQUET_TYPE_ILLEGAL``

In PySpark, collecting ``0001-01-01`` into a Python ``datetime`` failed with ``year 0 is out of range``. Cast the column to a string to see the value.

Troubleshoot the conversion
^^^^^^^^^^^^^^^^^^^^^^^^^^^
* ``KustoArrowNotInstalledError`` (an ``ImportError``): install pyarrow with ``pip install azure-kusto-data[pyarrow]``.
* ``ValueError: Unexpected type '...'``: the library doesn't know the type of a column.
  Pass ``converters_by_column_name`` or ``converters_by_type`` to convert it.
* Null instead of a value: by default (``errors="coerce"``), a value that can't be converted becomes null.
  Pass ``errors="raise"`` to get a ``ValueError`` that names the column and the value.
* Null datetimes with ``time_unit="ns"``: the datetimes are outside 1677-2262. Use the default ``time_unit="us"``.

Links
~~~~~

* `How to install the package <https://github.com/Azure/azure-kusto-python#install>`_.

* `Kusto query sample <https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py>`_.

* `GitHub Repository <https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-data>`_.
