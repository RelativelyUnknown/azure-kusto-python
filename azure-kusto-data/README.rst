Azure Kusto Data client library for Python
==========================================

Azure Data Explorer (Kusto) is a fast, fully managed data analytics service for real-time analysis on large volumes of data streaming from applications, websites, IoT devices, and more.

Use the client library for Azure Data Explorer to:

-  Run `Kusto Query Language (KQL) <https://learn.microsoft.com/kusto/query/>`__ queries and management commands against a cluster
-  Read the results row by row, or convert them to pandas DataFrames and Apache Arrow tables
-  Stream large results without loading them all into memory
-  Run queries and commands from asynchronous code

Links:

-  `Package (PyPI) <https://pypi.org/project/azure-kusto-data/>`__
-  `SDK documentation <https://learn.microsoft.com/kusto/api/python/kusto-python-client-library>`__
-  `Product documentation <https://learn.microsoft.com/azure/data-explorer/>`__
-  `Source code <https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-data>`__
-  `ChangeLog <https://github.com/Azure/azure-kusto-python/blob/master/CHANGELOG.md>`__
-  `Samples <https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py>`__

Getting started
---------------

Install the package
~~~~~~~~~~~~~~~~~~~

Install the Azure Kusto Data client library for Python with `pip <https://pypi.org/project/pip/>`__:

.. code:: bash

   pip install azure-kusto-data

Optional features have their own extras:

-  ``aio`` - the asynchronous client
-  ``pandas`` - conversion of results to pandas DataFrames
-  ``pyarrow`` - conversion of results to Apache Arrow tables and record batches

For example, ``pip install azure-kusto-data[pandas,pyarrow]``.

**Prerequisites**: You must have Python 3.9 or later, and an `Azure Data Explorer cluster and database <https://learn.microsoft.com/azure/data-explorer/create-cluster-and-database>`__ to use this package. A free cluster works.

Authenticate the client
~~~~~~~~~~~~~~~~~~~~~~~

Interaction with Kusto starts with a ``KustoConnectionStringBuilder``, which holds the URL of the cluster and the way to authenticate. Choose the authentication method that fits your scenario:

+----------------------------------+----------------------------------+
| Method                           | Use it to                        |
+==================================+==================================+
| ``with_az_cli_authentication``   | Sign in with the account of the  |
|                                  | `Azure                           |
|                                  | CLI <https://lea                 |
|                                  | rn.microsoft.com/cli/azure/>`__. |
|                                  | Run ``az login`` first.          |
+----------------------------------+----------------------------------+
| ``with_interactive_login``       | Sign in through a browser        |
|                                  | window.                          |
+----------------------------------+----------------------------------+
| ``                               | Sign in with a device code.      |
| with_aad_device_authentication`` |                                  |
+----------------------------------+----------------------------------+
| ``with_aad_                      | Authenticate a Microsoft Entra   |
| application_key_authentication`` | application with a secret.       |
+----------------------------------+----------------------------------+
| ``with_aad_app                   | Authenticate a Microsoft Entra   |
| lication_certificate_authenticat | application with a certificate.  |
| ion``\ \ ``with_aad_application_ |                                  |
| certificate_sni_authentication`` |                                  |
+----------------------------------+----------------------------------+
| ``with_aad_managed_s             | Authenticate with the managed    |
| ervice_identity_authentication`` | identity of an Azure resource.   |
+----------------------------------+----------------------------------+
| ``with_azure_token_credential``  | Authenticate with any            |
|                                  | ``azure-identity`` credential,   |
|                                  | such as                          |
|                                  | ``DefaultAzureCredential``.      |
+----------------------------------+----------------------------------+
| ``with_aa                        | Sign in with a user name and     |
| d_user_password_authentication`` | password. This doesn't work for  |
|                                  | accounts that require            |
|                                  | multi-factor authentication.     |
+----------------------------------+----------------------------------+
| ``with_token_provi               | Provide your own token.          |
| der``\ \ ``with_aad_user_token_a |                                  |
| uthentication``\ \ ``with_aad_ap |                                  |
| plication_token_authentication`` |                                  |
+----------------------------------+----------------------------------+
| ``with_no_authentication``       | Connect to a cluster that        |
|                                  | doesn't require authentication.  |
+----------------------------------+----------------------------------+

The `samples <https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py>`__ show every method.

Create the client
~~~~~~~~~~~~~~~~~

Create the client from the connection string builder. This example uses the account of the Azure CLI:

.. code:: python

   from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

   cluster = "https://<cluster-name>.<region>.kusto.windows.net"
   kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster)
   client = KustoClient(kcsb)

To authenticate a Microsoft Entra application with a secret, pass its application ID, secret, and tenant ID:

.. code:: python

   kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, "<application-id>", "<application-secret>", "<tenant-id>")

The client keeps a pool of connections, so create one client and reuse it. Close it with ``client.close()`` when you're done, or use it in a ``with`` block.

Create the asynchronous client
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Install the package with the ``aio`` extra. The asynchronous client has the same methods as the synchronous client, but lives in the ``azure.kusto.data.aio`` namespace, and you ``await`` its methods. Use it in an ``async with`` block, or close it with ``await client.close()``:

.. code:: python

   from azure.kusto.data import KustoConnectionStringBuilder
   from azure.kusto.data.aio import KustoClient

   kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster)


   async def main():
       async with KustoClient(kcsb) as client:
           ...

Key concepts
------------

-  **``KustoConnectionStringBuilder``** holds the URL of the cluster and the authentication method.
-  **``KustoClient``** sends queries and management commands to one cluster.

   -  ``execute`` runs a query, or a management command if the text starts with a dot (``.``).
   -  ``execute_query`` and ``execute_mgmt`` run a query or a management command explicitly.
   -  ``execute_streaming_query`` reads the response of a query while it arrives.

-  **Response**: the ``execute`` methods return a response. Its ``primary_results`` is a list of tables (``KustoResultTable``), one for each tabular statement of the query, and ``tables`` has all the tables, including the ones with information about the query. Iterate a table to get its rows (``KustoResultRow``), and read a value by position or by column name.
-  **Streaming response**: the tables of a streaming response are read from the network while you iterate them. You can read each table once, and you have to read a table to its end before you move to the next one.

Examples
--------

The following sections show some of the most common tasks. They use the ``client`` from `Create the client <#create-the-client>`__, and a database called ``Samples``:

-  `Run a query <#run-a-query>`__
-  `Read the rows of a result <#read-the-rows-of-a-result>`__
-  `Run a management command <#run-a-management-command>`__
-  `Set the properties of a request <#set-the-properties-of-a-request>`__
-  `Stream a large result <#stream-a-large-result>`__
-  `Run a query with the asynchronous client <#run-a-query-with-the-asynchronous-client>`__
-  `Convert a result to a pandas DataFrame <#convert-a-result-to-a-pandas-dataframe>`__
-  `Convert a result to an Arrow table <#convert-a-result-to-an-arrow-table>`__
-  `Stream a large result as Arrow record batches <#stream-a-large-result-as-arrow-record-batches>`__

Run a query
~~~~~~~~~~~

Use ``execute_query`` to run a query. The first primary result holds the rows of the query:

.. code:: python

   db = "Samples"
   response = client.execute_query(db, "StormEvents | take 10")
   table = response.primary_results[0]

Read the rows of a result
~~~~~~~~~~~~~~~~~~~~~~~~~

Iterate a table to get its rows. Read a value by position or by column name:

.. code:: python

   for row in response.primary_results[0]:
       print(row[0], row["EventType"])

Run a management command
~~~~~~~~~~~~~~~~~~~~~~~~

Use ``execute_mgmt`` to run a management command, such as ``.show tables``:

.. code:: python

   response = client.execute_mgmt(db, ".show tables")

Set the properties of a request
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``ClientRequestProperties`` to set options of a request, such as its timeout:

.. code:: python

   from datetime import timedelta

   from azure.kusto.data import ClientRequestProperties

   properties = ClientRequestProperties()
   properties.set_option(properties.request_timeout_option_name, timedelta(minutes=8))
   response = client.execute(db, "StormEvents", properties=properties)

Stream a large result
~~~~~~~~~~~~~~~~~~~~~

Use ``execute_streaming_query`` to read the rows while they arrive, instead of waiting for the entire response:

.. code:: python

   streaming_response = client.execute_streaming_query(db, "StormEvents")
   for table in streaming_response.iter_primary_results():
       for row in table:
           print(row["EventType"])

Run a query with the asynchronous client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With the asynchronous client, ``await`` the call:

.. code:: python

   response = await client.execute(db, "StormEvents | take 10")

Convert a result to a pandas DataFrame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Install the package with the ``pandas`` extra, and use ``dataframe_from_result_table``:

.. code:: python

   from azure.kusto.data.helpers import dataframe_from_result_table

   dataframe = dataframe_from_result_table(response.primary_results[0])

Convert a result to an Arrow table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Install the package with the ``pyarrow`` extra, and use ``arrow_table_from_result_table`` to convert a result to a ``pyarrow.Table``:

.. code:: python

   from azure.kusto.data.arrow_helpers import arrow_table_from_result_table

   arrow_table = arrow_table_from_result_table(response.primary_results[0])

Kusto counts time in ticks of 100 nanoseconds, and a datetime covers the years 0001-9999. By default, datetimes and timespans become Arrow microsecond types, which cover the whole range and are what most data tools expect, but drop the last digit of the fraction of a second. Pass ``time_unit="ns"`` to keep every tick. Arrow nanoseconds only cover the years 1677-2262, so a datetime outside that range becomes null.

The ``azure.kusto.data.arrow_helpers`` module documents the type mapping and the other options. For the streaming results of the asynchronous client, use ``azure.kusto.data.aio.arrow_helpers``.

Stream a large result as Arrow record batches
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``arrow_reader_from_result_table`` to convert a streaming result batch by batch. It returns a ``pyarrow.RecordBatchReader``:

.. code:: python

   from azure.kusto.data.arrow_helpers import arrow_reader_from_result_table

   streaming_response = client.execute_streaming_query(db, "StormEvents")
   reader = arrow_reader_from_result_table(next(streaming_response.iter_primary_results()))
   for batch in reader:
       print(batch.num_rows)

Troubleshooting
---------------

General
~~~~~~~

The library raises exceptions that derive from ``KustoError``. The most common ones are:

-  ``KustoServiceError``: the service couldn't process the request. Its subclasses include ``KustoApiError``, ``KustoMultiApiError``, and ``KustoNetworkError``.
-  ``KustoAuthenticationError``: the authentication failed.
-  ``KustoThrottlingError``: the service throttled the call.
-  ``KustoStreamingQueryError``: you moved to the next table of a streaming response before you read the current table to its end.
-  ``KustoClosedError``: you used a client that was closed.

Catch ``KustoServiceError`` to inspect an error of the service. This example runs a query with a semantic error:

.. code:: python

   from azure.kusto.data.exceptions import KustoServiceError

   try:
       client.execute(db, "StormEvents | where foo = bar")
   except KustoServiceError as error:
       print("Error:", error)
       print("Is semantic error:", error.is_semantic_error())
       print("Has partial results:", error.has_partial_results())

Converting results to Arrow
~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  ``KustoArrowNotInstalledError`` (an ``ImportError``): install pyarrow with ``pip install azure-kusto-data[pyarrow]``.
-  ``ValueError: Unexpected type '...'``: the library doesn't know the type of a column. Pass ``converters_by_column_name`` or ``converters_by_type`` to convert it.
-  Null instead of a value: by default (``errors="coerce"``), a value that can't be converted becomes null. Pass ``errors="raise"`` to get a ``ValueError`` that names the column and the value.
-  Null datetimes with ``time_unit="ns"``: the datetimes are outside the years 1677-2262. Use the default ``time_unit="us"``.
-  Spark, Azure Databricks, and Microsoft Fabric can't read Parquet files with nanosecond timestamps. Use the default ``time_unit="us"`` if you write the table to Parquet.

Next steps
----------

More sample code
~~~~~~~~~~~~~~~~

-  `Query samples <https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py>`__ - every authentication method, queries, streaming queries, DataFrames, and Arrow tables.
-  `Quick start app <https://github.com/Azure/azure-kusto-python/tree/master/quick-start>`__ - a runnable app that connects to a cluster, runs management commands, ingests data, and queries it.

Additional documentation
~~~~~~~~~~~~~~~~~~~~~~~~

-  `Azure Data Explorer documentation <https://learn.microsoft.com/azure/data-explorer/>`__
-  `Kusto Query Language (KQL) overview <https://learn.microsoft.com/kusto/query/>`__
-  `Kusto Python SDK <https://learn.microsoft.com/kusto/api/python/kusto-python-client-library>`__
-  To send data to a cluster, see the `azure-kusto-ingest <https://pypi.org/project/azure-kusto-ingest/>`__ package.

Contributing
------------

If you'd like to contribute to this library, read the `contributing guide <https://github.com/Azure/azure-kusto-python/blob/master/CONTRIBUTING.md>`__ to learn how to build and test the code.

This project has adopted the `Microsoft Open Source Code of Conduct <https://opensource.microsoft.com/codeofconduct/>`__. For more information, see the `Code of Conduct FAQ <https://opensource.microsoft.com/codeofconduct/faq/>`__.
