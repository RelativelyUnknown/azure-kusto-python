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



*Kusto Python Client* is a library for querying Kusto clusters from Python.
It supports all of Kusto's data types, and results can be read row by row, much like a DB API cursor.

It also works well from `Jupyter Notebooks <http://jupyter.org/>`_, including ones attached to Spark clusters, for instance on
`Azure Databricks <https://azure.microsoft.com/en-us/services/databricks/>`_.

Queries and control commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``client.execute`` runs a query, or a control command if the text starts with a dot (``.show tables``). If you want to be explicit, use
``execute_query`` or ``execute_mgmt``. The ``database`` argument may be ``None`` when the connection string names an "Initial Catalog".

Results are available as ``response.primary_results``, a list of tables you can iterate over. To pass options such as a timeout or query parameters,
use ``ClientRequestProperties``.

Pandas
~~~~~~
With the "pandas" extra (``pip install azure-kusto-data[pandas]``) a result table becomes a ``DataFrame``:

.. code:: python

    from azure.kusto.data.helpers import dataframe_from_result_table

    df = dataframe_from_result_table(response.primary_results[0])

Streaming queries
~~~~~~~~~~~~~~~~~
For results that are too big to hold in memory, ``execute_streaming_query`` returns the tables one at a time and the rows as you read them:

.. code:: python

    streaming_response = client.execute_streaming_query(db, "StormEvents")
    for table in streaming_response.iter_primary_results():
        for row in table:
            print(row["EventType"])

Async Client
~~~~~~~~~~~~
There is also an asynchronous client, in the ``azure.kusto.data.aio`` namespace. Install the package with the "aio" extra:

.. code:: bash

    pip install azure-kusto-data[aio]

It has the same interface as the regular client, except that its methods are coroutines, so you ``await`` them:

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
Results can also be converted to `Apache Arrow <https://arrow.apache.org/docs/python/>`_ tables and record batches. Install the package with the
"pyarrow" extra (``pip install azure-kusto-data[pyarrow]``):

.. code:: python

    from azure.kusto.data.arrow_helpers import arrow_table_from_result_table, arrow_reader_from_result_table

    table = arrow_table_from_result_table(response.primary_results[0])  # A pyarrow.Table

    # A streaming query is converted batch by batch, without reading the whole result into memory
    streaming_response = client.execute_streaming_query(db, "StormEvents")
    reader = arrow_reader_from_result_table(next(streaming_response.iter_primary_results()))  # A pyarrow.RecordBatchReader

Streaming results of the async client are converted with ``azure.kusto.data.aio.arrow_helpers``.

By default, Kusto's ``datetime`` and ``timespan`` values become Arrow timestamps and durations with microsecond resolution. That covers every Kusto
datetime (years 0001-9999) and is what most tools expect, but it drops the last digit (100 ns) of the fraction of a second. Pass ``time_unit="ns"`` to
keep it; Arrow's nanoseconds only cover the years 1677-2262, so anything outside that range becomes null.
See the documentation of ``azure.kusto.data.arrow_helpers`` for the type mapping and the other options.

Links
~~~~~

* `How to install the package <https://github.com/Azure/azure-kusto-python#install>`_.

* `Kusto query sample <https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py>`_.

* `GitHub Repository <https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-data>`_.
