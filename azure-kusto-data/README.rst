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
Query results can be converted to `Apache Arrow <https://arrow.apache.org/docs/python/>`_ tables and record batches, with all of Kusto's
data types. To use it, install the package with the "pyarrow" extra:

.. code:: bash

    pip install azure-kusto-data[pyarrow]

.. code:: python

    from azure.kusto.data.arrow_helpers import arrow_table_from_result_table, arrow_reader_from_result_table, arrow_tables_from_response

    response = client.execute(db, "StormEvents | take 10")
    table = arrow_table_from_result_table(response.primary_results[0])  # A pyarrow.Table
    tables = arrow_tables_from_response(response)  # A list with a pyarrow.Table for each primary result

    # A streaming query is converted batch by batch, without reading the entire result to memory
    streaming_response = client.execute_streaming_query(db, "StormEvents")
    reader = arrow_reader_from_result_table(next(streaming_response.iter_primary_results()))  # A pyarrow.RecordBatchReader

Kusto stores a datetime as a count of ticks of 100 nanoseconds and covers the years 0001-9999 (a timespan is counted in ticks as well).
By default they are converted to Arrow's microsecond resolution, like most data tools expect (Python, PyArrow, pandas 3, Polars, DuckDB,
Spark, Databricks, Delta and BigQuery). That covers the entire range, but drops the last digit of the fraction of a second.
Use ``time_unit="ns"`` to keep every tick. Arrow's nanoseconds only cover the years 1677-2262, so a datetime outside of this range, like
``datetime(9999-12-31)``, is converted to null, and Spark, Databricks and Fabric cannot read Parquet files with nanosecond timestamps.
See the documentation of ``azure.kusto.data.arrow_helpers`` for the type mapping and the other options.

Streaming results of the async client are converted with ``azure.kusto.data.aio.arrow_helpers``.

Links
~~~~~

* `How to install the package <https://github.com/Azure/azure-kusto-python#install>`_.

* `Kusto query sample <https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py>`_.

* `GitHub Repository <https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-data>`_.
