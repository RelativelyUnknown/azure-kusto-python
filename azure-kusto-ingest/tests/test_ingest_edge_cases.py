# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import io
import os
from unittest.mock import patch

import pytest
from pandas import DataFrame

from azure.kusto.data._telemetry import MonitoredActivity
from azure.kusto.data.data_format import DataFormat
from azure.kusto.data.exceptions import KustoBlobError
from azure.kusto.ingest import IngestionProperties, ManagedStreamingIngestClient, QueuedIngestClient
from azure.kusto.ingest.base_ingest_client import BaseIngestClient
from azure.kusto.ingest.descriptors import BlobDescriptor, StreamDescriptor
from azure.kusto.ingest.exceptions import KustoQueueError

ENDPOINT = "https://ingest-somecluster.kusto.windows.net"


class TestNothingToUpload:
    def test_no_containers(self):
        client = QueuedIngestClient(ENDPOINT)
        descriptor = StreamDescriptor(io.BytesIO(b"a,b"))
        with pytest.raises(KustoBlobError, match="no storage containers"):
            client.upload_blob([], descriptor, "database", "table", descriptor.stream, None, 10, 3)

    def test_no_queues(self):
        client = QueuedIngestClient(ENDPOINT)
        properties = IngestionProperties(database="database", table="table")
        with (
            patch.object(client._resource_manager, "get_ingestion_queues", return_value=[]),
            patch.object(client._resource_manager, "get_authorization_context", return_value="context"),
        ):
            with pytest.raises(KustoQueueError):
                client.ingest_from_blob(BlobDescriptor("https://account.blob.core.windows.net/c/b.csv?sas", 10), properties)


class TestIngestFromDataFrame:
    @pytest.mark.parametrize("data_format", [None, DataFormat.CSV, DataFormat.JSON])
    def test_properties_of_the_caller_are_not_modified(self, data_format):
        client = QueuedIngestClient(ENDPOINT)
        properties = IngestionProperties(database="database", table="table", data_format=DataFormat.TXT)
        with patch.object(QueuedIngestClient, "ingest_from_file") as ingest_from_file:
            client.ingest_from_dataframe(DataFrame({"a": [1]}), properties, data_format)
        assert properties.format == DataFormat.TXT
        sent_properties = ingest_from_file.call_args.args[1]
        assert sent_properties.format in (DataFormat.CSV, DataFormat.JSON)

    def test_temp_file_is_removed_when_writing_fails(self, tmp_path):
        class BrokenFrame(DataFrame):
            def to_json(self, *args, **kwargs):
                raise RuntimeError("cannot write")

        client = QueuedIngestClient(ENDPOINT)
        properties = IngestionProperties(database="database", table="table")
        with patch("tempfile.gettempdir", return_value=str(tmp_path)):
            with pytest.raises(RuntimeError):
                client.ingest_from_dataframe(BrokenFrame({"a": [1]}), properties)
        assert os.listdir(tmp_path) == []


class TestEndpoints:
    @pytest.mark.parametrize(
        "url, ingestion, query",
        [
            ("https://somecluster.kusto.windows.net", "https://ingest-somecluster.kusto.windows.net", "https://somecluster.kusto.windows.net"),
            ("https://ingest-somecluster.kusto.windows.net", "https://ingest-somecluster.kusto.windows.net", "https://somecluster.kusto.windows.net"),
            # "ingest-" has to be at the start of the host name, not anywhere in the URL
            ("https://bigingest-c.kusto.windows.net", "https://ingest-bigingest-c.kusto.windows.net", "https://bigingest-c.kusto.windows.net"),
            ("https://localhost:8080", "https://localhost:8080", "https://localhost:8080"),
            ("http://[::1]:8080", "http://[::1]:8080", "http://[::1]:8080"),
            ("https://192.168.14.4", "https://192.168.14.4", "https://192.168.14.4"),
        ],
    )
    def test_endpoints(self, url, ingestion, query):
        assert BaseIngestClient.get_ingestion_endpoint(url) == ingestion
        assert BaseIngestClient.get_query_endpoint(url) == query

    def test_only_localhost_is_reserved(self):
        assert BaseIngestClient.is_reserved_hostname("https://localhost:8080")
        assert BaseIngestClient.is_reserved_hostname("http://[::1]:8080")
        assert BaseIngestClient.is_reserved_hostname("https://onebox.dev.kusto.windows.net")
        assert not BaseIngestClient.is_reserved_hostname("https://mylocalhost-x.kusto.windows.net")


def test_managed_streaming_traces_the_attempt_number():
    client = ManagedStreamingIngestClient(ENDPOINT)
    properties = IngestionProperties(database="database", table="table")
    with patch.object(MonitoredActivity, "invoke", return_value="result") as invoke:
        client._stream_with_retries(1, StreamDescriptor(io.BytesIO(b"a")), properties)
    assert invoke.call_args.kwargs["tracing_attributes"]["attemptNumber"] == 1
