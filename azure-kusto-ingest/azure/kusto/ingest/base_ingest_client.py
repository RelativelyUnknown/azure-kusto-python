import copy
import gzip
import ipaddress
import os
import tempfile
import time
import uuid
from abc import ABCMeta, abstractmethod
from enum import Enum
from io import TextIOWrapper
from typing import TYPE_CHECKING, Any, Union, IO, AnyStr, Optional, Tuple, Dict
from urllib.parse import urlparse

from azure.kusto.data.data_format import DataFormat, IngestionMappingKind
from azure.kusto.data.exceptions import KustoClosedError
from .descriptors import FileDescriptor, StreamDescriptor
from .ingestion_properties import IngestionProperties

if TYPE_CHECKING:
    import pandas
    import pyarrow

INGEST_PREFIX = "ingest-"
PROTOCOL_SUFFIX = "://"


class IngestionStatus(Enum):
    """
    The ingestion was queued.
    """

    QUEUED = "QUEUED"
    """
    The ingestion was successfully streamed
    """
    SUCCESS = "SUCCESS"


class IngestionResult:
    """
    The result of an ingestion.
    """

    status: IngestionStatus
    "Will be `Queued` if the ingestion is queued, or `Success` if the ingestion is streaming and successful."

    database: str
    "The name of the database where the ingestion was performed."

    table: str
    "The name of the table where the ingestion was performed."

    source_id: uuid.UUID
    "The source id of the ingestion."

    blob_uri: Optional[str]
    "The blob uri of the ingestion, if exists."

    def __init__(self, status: IngestionStatus, database: str, table: str, source_id: uuid.UUID, blob_uri: Optional[str] = None):
        self.status = status
        self.database = database
        self.table = table
        self.source_id = source_id
        self.blob_uri = blob_uri

    def __repr__(self):
        # Remove query parameters from blob_uri, if exists
        obfuscated_path = None
        if isinstance(self.blob_uri, str):
            obfuscated_path = self.blob_uri.split("?")[0].split(";")[0]
        blob_uri = f", obfuscated_blob_uri={obfuscated_path}" if obfuscated_path else ""
        return f"IngestionResult(status={self.status}, database={self.database}, table={self.table}, source_id={self.source_id}{blob_uri})"


class BaseIngestClient(metaclass=ABCMeta):
    def __init__(self):
        self._is_closed: bool = False

    def ingest_from_file(self, file_descriptor: Union[FileDescriptor, str], ingestion_properties: IngestionProperties) -> IngestionResult:
        """Ingest from local files.
        :param file_descriptor: a FileDescriptor to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        if self._is_closed:
            raise KustoClosedError()

    @abstractmethod
    def ingest_from_stream(self, stream_descriptor: Union[StreamDescriptor, IO[AnyStr]], ingestion_properties: IngestionProperties) -> IngestionResult:
        """Ingest from io streams.
        :param stream_descriptor: An object that contains a description of the stream to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        if self._is_closed:
            raise KustoClosedError()

    @abstractmethod
    def set_proxy(self, proxy_url: str):
        """Set proxy for the ingestion client.
        :param str proxy_url: proxy url.
        """
        if self._is_closed:
            raise KustoClosedError()

    def ingest_from_dataframe(
        self, df: "pandas.DataFrame", ingestion_properties: IngestionProperties, data_format: Optional[DataFormat] = None
    ) -> IngestionResult:
        """Enqueue an ingest command from local files.
        To learn more about ingestion methods go to:
        https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
        :param pandas.DataFrame df: input dataframe to ingest.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        :param DataFormat data_format: Format to convert the dataframe to - Can be DataFormat.CSV, DataFormat.JSON or None. If not specified, it will try to infer it from the mapping, if not found, it will default to JSON.
        """

        if self._is_closed:
            raise KustoClosedError()

        from pandas import DataFrame

        if not isinstance(df, DataFrame):
            raise ValueError("Expected DataFrame instance, found {}".format(type(df)))

        is_json = True

        # If we are given CSV mapping, or the mapping format is explicitly set to CSV, we should use CSV
        if not data_format:
            if ingestion_properties is not None and (ingestion_properties.ingestion_mapping_type == IngestionMappingKind.CSV):
                is_json = False
        elif data_format == DataFormat.CSV:
            is_json = False
        elif data_format == DataFormat.JSON:
            is_json = True
        else:
            raise ValueError("Unsupported format: {}. Supported formats are: CSV, JSON, None".format(data_format))

        file_name = "df_{id}_{timestamp}_{uid}.{ext}.gz".format(id=id(df), timestamp=int(time.time()), uid=uuid.uuid4(), ext="json" if is_json else "csv")
        temp_file_path = os.path.join(tempfile.gettempdir(), file_name)
        with gzip.open(temp_file_path, "wt", encoding="utf-8") as temp_file:
            if is_json:
                df.to_json(temp_file, orient="records", date_format="iso", lines=True)
                ingestion_properties.format = DataFormat.JSON
            else:
                df.to_csv(temp_file, index=False, encoding="utf-8", header=False)
                ingestion_properties.ignore_first_record = False
                ingestion_properties.format = DataFormat.CSV

        try:
            return self.ingest_from_file(temp_file_path, ingestion_properties)
        finally:
            os.unlink(temp_file_path)

    def ingest_from_arrow(
        self,
        data: "Union[pyarrow.Table, pyarrow.RecordBatch, pyarrow.RecordBatchReader, Any]",
        ingestion_properties: IngestionProperties,
        *,
        compression: str = "snappy",
        row_group_size: Optional[int] = None,
        parquet_options: Optional[Dict[str, Any]] = None,
    ) -> IngestionResult:
        """Ingests Arrow data. Writes the data to a Parquet file, and ingests the file like any other file.
        To learn more about ingestion methods, see
        https://learn.microsoft.com/azure/data-explorer/ingest-data-overview#ingestion-methods

        Columns are matched to the columns of the table by name, unless you provide a mapping. Kusto converts the Arrow types this way:
        a timestamp to a datetime, an integer to an int or a long, a floating-point number to a real, a decimal to a decimal,
        a string to a string, a nested type (a list, struct, or map) to a dynamic, and a uuid to a guid. A uuid requires pyarrow 21 or later.
        Kusto doesn't convert a string to a guid, a datetime, or a timespan. It can't ingest a timespan from a Parquet file,
        so duration columns are rejected, and so are null columns.

        Timestamps are written with microsecond resolution, which every Parquet reader supports. Spark, Azure Databricks, and Microsoft
        Fabric can't read nanoseconds. A timestamp with nanoseconds is truncated to microseconds. To change that, pass `parquet_options`.
        For example, `{"coerce_timestamps": None}` writes nanoseconds as they are, which helps only if the reader accepts them.

        To ingest every 100-nanosecond tick of a datetime:

        1. Cast the column to int64 nanoseconds since the epoch.
        2. Create the properties with `data_format=DataFormat.PARQUET` and a Parquet mapping that converts the column:
           `ColumnMapping(column_name="ts", column_type="datetime", path="$.ts", transform=TransformationMethod.DATE_TIME_FROM_UNIX_NANOSECONDS)`.

        Without the mapping, an integer column that reaches a datetime column is ingested as empty or wrong data. Streaming ingestion
        doesn't accept a mapping in the request. With the streaming and managed streaming clients, create the mapping beforehand and pass
        its name as `ingestion_mapping_reference`.

        A RecordBatchReader or another stream is written batch by batch, so it doesn't have to fit in memory.
        The Parquet file is temporary, and is deleted after the ingestion.

        :param data: The data to ingest. A pyarrow Table, RecordBatch, or RecordBatchReader, or any object that implements
            the Arrow PyCapsule stream interface (`__arrow_c_stream__`), such as a Polars dataframe.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties. The format is always Parquet. The client
            sets it on a copy of the properties, so the object you pass isn't modified. A mapping has to be a Parquet mapping, which it
            is if you create the properties with `data_format=DataFormat.PARQUET`.
        :param compression: The compression codec of the Parquet file: "none", "snappy", "gzip", "brotli", "lz4", or "zstd".
            Defaults to "snappy".
        :param row_group_size: The maximal number of rows in each row group of the Parquet file. Defaults to None, which uses the pyarrow
            default.
        :param parquet_options: Additional options for `pyarrow.parquet.ParquetWriter`. They take precedence over the defaults
            (`coerce_timestamps="us"` and `allow_truncated_timestamps=True`), for example `{"coerce_timestamps": "ms"}`.
            Defaults to None.
        :raises azure.kusto.data.exceptions.KustoClosedError: If the client is closed.
        :raises azure.kusto.data.exceptions.KustoArrowNotInstalledError: If pyarrow isn't installed.
        :raises ValueError: If `data` isn't Arrow data, has no columns, or has a column that Kusto can't ingest from Parquet (a duration,
            time, or null column, or a uuid column with a pyarrow older than 21), or if the ingestion mapping isn't a Parquet mapping.
        :return: The result of the ingestion of the Parquet file.
        """

        if self._is_closed:
            raise KustoClosedError()

        # A mapping which is not a Parquet mapping would be sent to the service together with the Parquet format
        if ingestion_properties.ingestion_mapping_type not in (None, IngestionMappingKind.PARQUET):
            raise ValueError(
                "Arrow data is ingested as Parquet, but the ingestion mapping is a {} mapping. Create the IngestionProperties with data_format=DataFormat.PARQUET".format(
                    ingestion_properties.ingestion_mapping_type.value
                )
            )

        from ._arrow_ingest import write_parquet

        file_path = write_parquet(data, compression, row_group_size, parquet_options)

        parquet_properties = copy.copy(ingestion_properties)
        parquet_properties.format = DataFormat.PARQUET
        try:
            return self.ingest_from_file(file_path, parquet_properties)
        finally:
            os.unlink(file_path)

    @staticmethod
    def _prepare_stream(stream_descriptor: Union[StreamDescriptor, IO[AnyStr]], ingestion_properties: IngestionProperties) -> StreamDescriptor:
        """
        Prepares a StreamDescriptor instance for ingest operation based on ingestion properties
        :param StreamDescriptor stream_descriptor: Stream descriptor instance
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        :return prepared stream descriptor
        """
        new_descriptor = StreamDescriptor.get_instance(stream_descriptor)

        if isinstance(new_descriptor.stream, TextIOWrapper):
            new_descriptor.stream = new_descriptor.stream.buffer

        should_compress = BaseIngestClient._should_compress(new_descriptor, ingestion_properties)
        if should_compress:
            new_descriptor.compress_stream()

        return new_descriptor

    @staticmethod
    def _prepare_file(file_descriptor: Union[FileDescriptor, str], ingestion_properties: IngestionProperties) -> Tuple[FileDescriptor, bool]:
        """
        Prepares a FileDescriptor instance for ingest operation based on ingestion properties
        :param FileDescriptor file_descriptor: File descriptor instance
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        :return prepared file descriptor
        """
        descriptor = FileDescriptor.get_instance(file_descriptor)

        should_compress = BaseIngestClient._should_compress(descriptor, ingestion_properties)
        return descriptor, should_compress

    @staticmethod
    def _should_compress(new_descriptor: Union[FileDescriptor, StreamDescriptor], ingestion_properties: IngestionProperties) -> bool:
        """
        Checks if descriptor should be compressed based on ingestion properties and current format
        """
        return not new_descriptor.is_compressed and ingestion_properties.format.compressible

    def close(self) -> None:
        self._is_closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def get_ingestion_endpoint(cluster_url: str) -> str:
        if INGEST_PREFIX in cluster_url or not cluster_url or BaseIngestClient.is_reserved_hostname(cluster_url):
            return cluster_url
        else:
            return cluster_url.replace(PROTOCOL_SUFFIX, PROTOCOL_SUFFIX + INGEST_PREFIX, 1)

    @staticmethod
    def get_query_endpoint(cluster_url: str) -> str:
        if INGEST_PREFIX in cluster_url:
            return cluster_url.replace(INGEST_PREFIX, "", 1)
        else:
            return cluster_url

    @staticmethod
    def is_reserved_hostname(raw_uri: str) -> bool:
        url = urlparse(raw_uri)
        if not url.netloc:
            return True
        authority = url.netloc.split(":")[0]  # removes port if exists
        try:
            is_ip = ipaddress.ip_address(authority)
        except ValueError:
            is_ip = False
        is_localhost = "localhost" in authority
        return is_localhost or is_ip or authority.lower() == "onebox.dev.kusto.windows.net"
