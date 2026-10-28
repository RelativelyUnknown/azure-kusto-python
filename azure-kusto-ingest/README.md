# Azure Kusto Ingest client library for Python

Azure Data Explorer (Kusto) is a fast, fully managed data analytics service for real-time analysis on large volumes of data streaming from applications, websites, IoT devices, and more.

Use the client library for Azure Data Explorer to:

* Queue the ingestion of files, blobs, and streams into a table
* Ingest pandas DataFrames and Apache Arrow data
* Send small amounts of data straight to the engine with streaming ingestion
* Map the columns of the source to the columns of the table

  - [Package (PyPI)](https://pypi.org/project/azure-kusto-ingest/)
  - [SDK documentation](https://learn.microsoft.com/kusto/api/python/kusto-python-client-library)
  - [Product documentation](https://learn.microsoft.com/azure/data-explorer/)
  - [Source code](https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-ingest)
  - [ChangeLog](https://github.com/Azure/azure-kusto-python/blob/master/CHANGELOG.md)
  - [Samples](https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-ingest/tests/sample.py)

## Getting started

### Install the package

Install the Azure Kusto Ingest client library for Python with [pip](https://pypi.org/project/pip/):

```bash
pip install azure-kusto-ingest
```

Optional features have their own extras:

* `pandas` - ingestion of pandas DataFrames
* `pyarrow` - ingestion of Apache Arrow data

For example, `pip install azure-kusto-ingest[pandas,pyarrow]`.

**Prerequisites**: You must have Python 3.9 or later, and an [Azure Data Explorer cluster and database](https://learn.microsoft.com/azure/data-explorer/create-cluster-and-database) with a table to ingest into. You also need [permission to ingest](https://learn.microsoft.com/azure/data-explorer/manage-database-permissions) into that database.

### Authenticate the client

The ingest clients authenticate the same way as the query client: create a `KustoConnectionStringBuilder` with one of its authentication methods. The [azure-kusto-data README](https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-data#authenticate-the-client) describes every method.

### Create the client

Create a `QueuedIngestClient` from the connection string builder. This example uses the account of the Azure CLI:

```python
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.ingest import QueuedIngestClient

cluster = "https://<cluster-name>.<region>.kusto.windows.net"
kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster)
client = QueuedIngestClient(kcsb)
```

By default, the clients correct the endpoint for their type: the queued client uses the ingestion endpoint of the cluster (the name with the `ingest-` prefix), and the streaming clients use the engine endpoint. You can pass either one.

Reuse one ingest client for each target database in a process, as the [best practices](https://learn.microsoft.com/kusto/api/netfx/kusto-ingest-best-practices) recommend. Close the client with `client.close()` when you're done.

## Key concepts

* **`QueuedIngestClient`** uploads the data to Azure Storage and queues a message. The service ingests the data later, in batches. Use it for production scenarios and large volumes of data.
* **`KustoStreamingIngestClient`** sends the data straight to the engine, which makes it available quickly. Use it for small amounts of data, up to 4 MB for each ingestion. [Streaming ingestion has to be enabled](https://learn.microsoft.com/azure/data-explorer/ingest-data-streaming) on the cluster.
* **`ManagedStreamingIngestClient`** tries streaming ingestion and falls back to queued ingestion, for example when the data is too large for streaming ingestion.
* **`IngestionProperties`** describes where and how to ingest: the database, the table, the data format (`DataFormat`), the column mapping, and more.
* **`FileDescriptor`, `BlobDescriptor`, and `StreamDescriptor`** wrap the source of the data and, optionally, its raw size. The ingest methods also accept a file path or a stream directly.
* **`IngestionResult`** is what an ingest method returns. Its `status` is `IngestionStatus.QUEUED` when the data was queued for ingestion, and `IngestionStatus.SUCCESS` when it was streamed.

## Examples

The following sections show some of the most common tasks. They use the `client` from [Create the client](#create-the-client) and these ingestion properties:

```python
from azure.kusto.data import DataFormat
from azure.kusto.ingest import IngestionProperties

ingestion_props = IngestionProperties(database="<database-name>", table="<table-name>", data_format=DataFormat.CSV)
```

* [Ingest a file](#ingest-a-file)
* [Ingest a blob](#ingest-a-blob)
* [Ingest a stream](#ingest-a-stream)
* [Ingest a pandas DataFrame](#ingest-a-pandas-dataframe)
* [Ingest Arrow data](#ingest-arrow-data)
* [Keep every tick of an Arrow timestamp](#keep-every-tick-of-an-arrow-timestamp)
* [Map the columns of the source](#map-the-columns-of-the-source)
* [Use streaming ingestion](#use-streaming-ingestion)
* [Use managed streaming ingestion](#use-managed-streaming-ingestion)

### Ingest a file

Use `ingest_from_file` with the path of a file. To give the raw (uncompressed) size of the data in bytes, pass a `FileDescriptor`. An exact size helps to avoid extra storage transactions:

```python
from azure.kusto.ingest import FileDescriptor

result = client.ingest_from_file("data.csv", ingestion_properties=ingestion_props)
client.ingest_from_file(FileDescriptor("data.csv", 3333), ingestion_properties=ingestion_props)
```

### Ingest a blob

Use `ingest_from_blob` with the URL of a blob, including a SAS token, and the raw size of the data in bytes:

```python
from azure.kusto.ingest import BlobDescriptor

blob_descriptor = BlobDescriptor("https://<account>.blob.core.windows.net/<container>/data.csv.gz?<sas-token>", 51200)
client.ingest_from_blob(blob_descriptor, ingestion_properties=ingestion_props)
```

### Ingest a stream

Use `ingest_from_stream` with a binary or text stream:

```python
import io

client.ingest_from_stream(io.StringIO("1,abc,15.3\n2,cde,99.9\n"), ingestion_properties=ingestion_props)
```

### Ingest a pandas DataFrame

Install the package with the `pandas` extra, and use `ingest_from_dataframe`. The client converts the DataFrame to JSON, or to CSV if you pass `data_format=DataFormat.CSV` or a CSV mapping. It sets that format on the properties you pass in, so create separate properties for DataFrames:

```python
import pandas

dataframe = pandas.DataFrame(data=[[1, "abc", 15.3], [2, "cde", 99.9]], columns=["id", "name", "value"])
dataframe_props = IngestionProperties(database="<database-name>", table="<table-name>", data_format=DataFormat.JSON)
client.ingest_from_dataframe(dataframe, ingestion_properties=dataframe_props)
```

### Ingest Arrow data

Install the package with the `pyarrow` extra, and use `ingest_from_arrow`. It takes a `pyarrow.Table`, `RecordBatch`, or `RecordBatchReader`, or any object that implements the Arrow PyCapsule stream interface. The client writes the data to a Parquet file and ingests the file, so create the properties with the Parquet format:

```python
import pyarrow

arrow_table = pyarrow.table({"id": [1, 2], "name": ["abc", "cde"], "value": [15.3, 99.9]})
parquet_props = IngestionProperties(database="<database-name>", table="<table-name>", data_format=DataFormat.PARQUET)
client.ingest_from_arrow(arrow_table, ingestion_properties=parquet_props)
```

A `RecordBatchReader` is written batch by batch, so it doesn't have to fit in memory. Kusto can't ingest a timespan from a Parquet file, so `duration` columns are rejected.

The client writes timestamps with microsecond resolution, which every Parquet reader supports. A `timestamp[ns]` column is truncated to microseconds. To write nanoseconds as they are, pass `parquet_options={"coerce_timestamps": None}`. The Kusto documentation doesn't say which timestamp units its Parquet reader accepts.

### Keep every tick of an Arrow timestamp

A Kusto datetime counts ticks of 100 nanoseconds. To keep every tick, cast the timestamp column to `int64` (nanoseconds since 1970), and use a Parquet mapping with the `DateTimeFromUnixNanoseconds` transformation to convert it back:

```python
from azure.kusto.ingest import ColumnMapping, TransformationMethod

arrow_table = pyarrow.table({"id": [1], "when": pyarrow.array([1_700_000_000_123_456_700], pyarrow.timestamp("ns", "UTC"))})
arrow_table = arrow_table.set_column(1, "when", arrow_table.column("when").cast(pyarrow.int64()))

mapped_props = IngestionProperties(
    database="<database-name>",
    table="<table-name>",
    data_format=DataFormat.PARQUET,
    column_mappings=[
        ColumnMapping(column_name="id", column_type="long", path="$.id"),
        ColumnMapping(column_name="when", column_type="datetime", path="$.when", transform=TransformationMethod.DATE_TIME_FROM_UNIX_NANOSECONDS),
    ],
)
client.ingest_from_arrow(arrow_table, ingestion_properties=mapped_props)
```

Without the mapping, an integer column that goes into a datetime column is ingested as empty or wrong data. Streaming ingestion doesn't accept a mapping in the request. With the streaming clients, create the mapping beforehand and pass its name as `ingestion_mapping_reference`.

### Map the columns of the source

When the columns of the source don't match the columns of the table, use an [ingestion mapping](https://learn.microsoft.com/kusto/management/mappings). Create the mapping on the table beforehand, and refer to it by name:

```python
from azure.kusto.ingest import IngestionMappingKind

json_props = IngestionProperties(
    database="<database-name>",
    table="<table-name>",
    data_format=DataFormat.JSON,
    ingestion_mapping_reference="<mapping-name>",
    ingestion_mapping_kind=IngestionMappingKind.JSON,
)
```

The queued client also accepts a list of `ColumnMapping` objects as `column_mappings`.

### Use streaming ingestion

Create a `KustoStreamingIngestClient` for a cluster or database with streaming ingestion enabled:

```python
from azure.kusto.ingest import KustoStreamingIngestClient

streaming_client = KustoStreamingIngestClient(kcsb)
streaming_client.ingest_from_file("data.csv", ingestion_properties=ingestion_props)
```

### Use managed streaming ingestion

Create a `ManagedStreamingIngestClient`. It streams when it can, and queues the data otherwise. The status of the result tells which one happened:

```python
from azure.kusto.ingest import IngestionStatus, ManagedStreamingIngestClient

managed_client = ManagedStreamingIngestClient(kcsb)
result = managed_client.ingest_from_file("data.csv", ingestion_properties=ingestion_props)
if result.status == IngestionStatus.QUEUED:
    print("The data was queued for ingestion.")
```

## Troubleshooting

### General

The library raises exceptions that derive from `KustoError`. The most common ones for ingestion are:

* `KustoInvalidEndpointError`: the endpoint isn't of the type that the client needs. The message suggests the right endpoint.
* `KustoMappingError`, `KustoMissingMappingError`, and `KustoDuplicateMappingError`: the ingestion mapping is invalid, missing, or given twice (as a reference and as a list of columns).
* `KustoQueueError`: the client couldn't queue the message after all the retries.
* `KustoServiceError`: the service rejected the request, for example in streaming ingestion.
* `KustoClosedError`: you used a client that was closed.

A queued ingestion happens after the call returns, so `IngestionStatus.QUEUED` doesn't mean that the data is in the table. To learn the outcome, set `report_level=ReportLevel.FailuresAndSuccesses` in the ingestion properties, and read the status queues with `KustoIngestStatusQueues`. The [samples](https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-ingest/tests/sample.py) show how.

### Ingesting Arrow data

* `KustoArrowNotInstalledError` (an `ImportError`): install pyarrow with `pip install azure-kusto-ingest[pyarrow]`.
* `ValueError` about a duration column: Kusto can't ingest a timespan from a Parquet file. Cast the column to a string or to an integer, such as the number of ticks, and convert it after the ingestion.
* `ValueError` about a uuid column: pyarrow versions before 21 don't write the Parquet UUID type that Kusto needs for a guid. Upgrade pyarrow.
* `ValueError` about the ingestion mapping: Arrow data is ingested as Parquet. Create the `IngestionProperties` with `data_format=DataFormat.PARQUET`.

## Next steps

### More sample code

* [Ingestion samples](https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-ingest/tests/sample.py) - queued, streaming, and managed streaming ingestion, DataFrames, Arrow data, and the ingestion status.
* [Quick start app](https://github.com/Azure/azure-kusto-python/tree/master/quick-start) - a runnable app that connects to a cluster, runs management commands, ingests data, and queries it.

### Additional documentation

* [Data ingestion overview](https://learn.microsoft.com/azure/data-explorer/ingest-data-overview)
* [Best practices for the Kusto Ingest library](https://learn.microsoft.com/kusto/api/netfx/kusto-ingest-best-practices)
* [Streaming ingestion](https://learn.microsoft.com/azure/data-explorer/ingest-data-streaming)
* To query a cluster, see the [azure-kusto-data](https://pypi.org/project/azure-kusto-data/) package.

## Contributing

If you'd like to contribute to this library, read the [contributing guide](https://github.com/Azure/azure-kusto-python/blob/master/CONTRIBUTING.md) to learn how to build and test the code.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information, see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/).
