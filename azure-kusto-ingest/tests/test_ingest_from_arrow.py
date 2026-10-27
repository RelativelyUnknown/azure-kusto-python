# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import datetime
import os
import sys
import uuid
from decimal import Decimal

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from azure.kusto.data.data_format import DataFormat, IngestionMappingKind  # noqa: E402
from azure.kusto.data.exceptions import KustoArrowNotInstalledError, KustoClosedError  # noqa: E402
from azure.kusto.ingest import IngestionProperties, IngestionStatus  # noqa: E402
from azure.kusto.ingest.base_ingest_client import BaseIngestClient, IngestionResult  # noqa: E402
from azure.kusto.ingest.descriptors import FileDescriptor  # noqa: E402
from azure.kusto.ingest.ingestion_properties import ColumnMapping  # noqa: E402


class RecordingClient(BaseIngestClient):
    """Records what is ingested from a file, reading the file while it exists"""

    def __init__(self, fail_with=None):
        super().__init__()
        self.fail_with = fail_with
        self.ingestions = []

    def ingest_from_stream(self, stream_descriptor, ingestion_properties):
        raise NotImplementedError

    def set_proxy(self, proxy_url):
        raise NotImplementedError

    def ingest_from_file(self, file_descriptor, ingestion_properties):
        super().ingest_from_file(file_descriptor, ingestion_properties)
        path = FileDescriptor.get_instance(file_descriptor).path
        self.ingestions.append(
            {
                "path": path,
                "name": os.path.basename(path),
                "format": ingestion_properties.format,
                "file": pq.ParquetFile(path).metadata,
                "table": pq.read_table(path),
            }
        )
        if self.fail_with:
            raise self.fail_with
        return IngestionResult(IngestionStatus.QUEUED, ingestion_properties.database, ingestion_properties.table, uuid.uuid4())

    @property
    def last(self):
        return self.ingestions[-1]


@pytest.fixture
def client():
    return RecordingClient()


@pytest.fixture
def properties():
    return IngestionProperties(database="database", table="table")


def sample_table(rows: int = 5) -> "pa.Table":
    return pa.table(
        {
            "id": pa.array(range(rows), type=pa.int64()),
            "name": pa.array(["name{}".format(i) if i % 2 == 0 else None for i in range(rows)], type=pa.string()),
            "value": pa.array([i * 1.5 for i in range(rows)], type=pa.float64()),
            "flag": pa.array([i % 2 == 0 for i in range(rows)], type=pa.bool_()),
            "when": pa.array([datetime.datetime(2021, 12, 22, 11, 43, i, tzinfo=datetime.timezone.utc) for i in range(rows)], type=pa.timestamp("us", "UTC")),
            "dynamic": pa.array([[i, i + 1] for i in range(rows)], type=pa.list_(pa.int64())),
        }
    )


class TestIngestFromArrow:
    def test_table(self, client, properties):
        table = sample_table()
        result = client.ingest_from_arrow(table, properties)
        assert result.status == IngestionStatus.QUEUED

        ingestion = client.last
        assert ingestion["format"] == DataFormat.PARQUET
        assert ingestion["name"].startswith("arrow_") and ingestion["name"].endswith(".parquet")
        assert ingestion["table"].equals(table)
        assert not os.path.exists(ingestion["path"])

    def test_the_given_properties_are_not_modified(self, client, properties):
        assert properties.format == DataFormat.CSV
        client.ingest_from_arrow(sample_table(), properties)
        assert properties.format == DataFormat.CSV

    def test_record_batch(self, client, properties):
        batch = sample_table().to_batches()[0]
        client.ingest_from_arrow(batch, properties)
        assert client.last["table"].equals(pa.Table.from_batches([batch]))

    def test_record_batch_reader_is_written_batch_by_batch(self, client, properties):
        table = sample_table(10)
        reader = pa.RecordBatchReader.from_batches(table.schema, iter(table.to_batches(max_chunksize=3)))
        client.ingest_from_arrow(reader, properties)
        assert client.last["table"].equals(table)

    def test_object_with_the_arrow_stream_interface(self, client, properties):
        table = sample_table()

        class Frame:
            def __arrow_c_stream__(self, requested_schema=None):
                return table.__arrow_c_stream__(requested_schema)

        client.ingest_from_arrow(Frame(), properties)
        assert client.last["table"].equals(table)

    def test_empty_table(self, client, properties):
        client.ingest_from_arrow(sample_table(0), properties)
        assert client.last["table"].num_rows == 0
        assert client.last["table"].schema.names == sample_table(0).schema.names

    def test_compression_and_row_groups(self, client, properties):
        client.ingest_from_arrow(sample_table(10), properties)
        assert client.last["file"].row_group(0).column(0).compression == "SNAPPY"
        assert client.last["file"].num_row_groups == 1

        client.ingest_from_arrow(sample_table(10), properties, compression="zstd", row_group_size=4)
        assert client.last["file"].row_group(0).column(0).compression == "ZSTD"
        assert client.last["file"].num_row_groups == 3

        client.ingest_from_arrow(sample_table(10), properties, parquet_options={"compression": "gzip"})
        assert client.last["file"].row_group(0).column(0).compression == "GZIP"

    def test_types_survive_the_round_trip(self, client, properties):
        table = pa.table(
            {
                "nanoseconds": pa.array([1, None], pa.timestamp("ns", "UTC")),
                "decimal": pa.array([Decimal("1.5"), None], pa.decimal128(38, 18)),
                "text": pa.array(["a", None]),
                "number": pa.array([1, None], pa.int32()),
                "nested": pa.array([{"a": 1}, None]),
            }
        )
        client.ingest_from_arrow(table, properties)
        assert client.last["table"].schema.field("nanoseconds").type == pa.timestamp("ns", "UTC")
        assert client.last["table"].equals(table)

    def test_timestamp_resolution_can_be_coerced(self, client, properties):
        table = pa.table({"when": pa.array([1_000_000_123], pa.timestamp("ns", "UTC"))})
        client.ingest_from_arrow(table, properties, parquet_options={"coerce_timestamps": "us", "allow_truncated_timestamps": True})
        assert client.last["table"].schema.field("when").type == pa.timestamp("us", "UTC")

    def test_uuid(self, client, properties):
        if int(pa.__version__.split(".")[0]) < 21:
            pytest.skip("Older versions of pyarrow don't write the Parquet UUID type")
        guid = uuid.UUID("6f3c1072-2739-461c-8aa7-3cfc8ff528a8")
        table = pa.table({"guid": pa.ExtensionArray.from_storage(pa.uuid(), pa.array([guid.bytes, None], pa.binary(16)))})
        client.ingest_from_arrow(table, properties)
        assert client.last["table"].column("guid").to_pylist() == [guid, None]

    def test_uuid_needs_a_new_pyarrow(self, client, properties, monkeypatch):
        if not hasattr(pa, "uuid"):
            pytest.skip("No uuid type")
        table = pa.table({"guid": pa.ExtensionArray.from_storage(pa.uuid(), pa.array([uuid.uuid4().bytes], pa.binary(16)))})
        monkeypatch.setattr(pa, "__version__", "20.0.0")
        with pytest.raises(ValueError, match="column 'guid' is of the uuid type.*version 21"):
            client.ingest_from_arrow(table, properties)
        assert client.ingestions == []

    @pytest.mark.parametrize(
        "column, message",
        [
            (pa.array([1], pa.duration("ns")), "is of the type duration"),
            (pa.array([1], pa.duration("us")), "is of the type duration"),
            (pa.array([1], pa.time64("us")), "is of the type time64"),
            (pa.array([1], pa.time32("s")), "is of the type time32"),
            (pa.array([None]), "is of the null type"),
        ],
    )
    def test_types_kusto_can_not_ingest(self, client, properties, column, message):
        with pytest.raises(ValueError, match="column 'c' " + message):
            client.ingest_from_arrow(pa.table({"ok": [1], "c": column}), properties)
        assert client.ingestions == []

    def test_all_the_problems_are_reported(self, client, properties):
        with pytest.raises(ValueError) as error:
            client.ingest_from_arrow(pa.table({"a": pa.array([1], pa.duration("ns")), "b": pa.array([None])}), properties)
        assert "column 'a'" in str(error.value) and "column 'b'" in str(error.value)

    def test_a_table_without_columns(self, client, properties):
        with pytest.raises(ValueError, match="no columns"):
            client.ingest_from_arrow(pa.table({}), properties)

    @pytest.mark.parametrize("data", [None, [1, 2], {"a": [1]}, "table", 5])
    def test_invalid_input(self, client, properties, data):
        with pytest.raises(ValueError, match="Expected a pyarrow Table"):
            client.ingest_from_arrow(data, properties)

    def test_mapping(self, client):
        mapping = [ColumnMapping(column_name="id", column_type="long", path="$.id")]
        properties = IngestionProperties(database="database", table="table", data_format=DataFormat.PARQUET, column_mappings=mapping)
        assert properties.ingestion_mapping_type == IngestionMappingKind.PARQUET
        client.ingest_from_arrow(sample_table(), properties)
        assert client.last["format"] == DataFormat.PARQUET

        properties = IngestionProperties(database="database", table="table", data_format=DataFormat.PARQUET, ingestion_mapping_reference="mapping")
        client.ingest_from_arrow(sample_table(), properties)

    @pytest.mark.parametrize("data_format", [DataFormat.CSV, DataFormat.JSON])
    def test_mapping_of_another_kind(self, client, data_format):
        properties = IngestionProperties(database="database", table="table", data_format=data_format, ingestion_mapping_reference="mapping")
        with pytest.raises(ValueError, match="data_format=DataFormat.PARQUET"):
            client.ingest_from_arrow(sample_table(), properties)
        assert client.ingestions == []

    def test_closed_client(self, client, properties):
        client.close()
        with pytest.raises(KustoClosedError):
            client.ingest_from_arrow(sample_table(), properties)

    def test_file_is_deleted_when_the_ingestion_fails(self, properties):
        client = RecordingClient(fail_with=RuntimeError("ingestion failed"))
        with pytest.raises(RuntimeError, match="ingestion failed"):
            client.ingest_from_arrow(sample_table(), properties)
        assert not os.path.exists(client.last["path"])

    def test_file_is_deleted_when_reading_the_data_fails(self, client, properties, tmp_path, monkeypatch):
        monkeypatch.setattr("tempfile.gettempdir", lambda: str(tmp_path))
        table = sample_table(6)

        def batches():
            yield table.to_batches()[0]
            raise RuntimeError("the stream failed")

        reader = pa.RecordBatchReader.from_batches(table.schema, batches())
        with pytest.raises(RuntimeError, match="the stream failed"):
            client.ingest_from_arrow(reader, properties)
        assert list(tmp_path.iterdir()) == []
        assert client.ingestions == []

    def test_file_is_deleted_when_writing_fails(self, client, properties, tmp_path, monkeypatch):
        monkeypatch.setattr("tempfile.gettempdir", lambda: str(tmp_path))
        with pytest.raises(Exception):
            client.ingest_from_arrow(sample_table(), properties, parquet_options={"compression": "no such codec"})
        assert list(tmp_path.iterdir()) == []

    def test_files_are_deleted_after_the_ingestion(self, client, properties, tmp_path, monkeypatch):
        monkeypatch.setattr("tempfile.gettempdir", lambda: str(tmp_path))
        client.ingest_from_arrow(sample_table(), properties)
        assert list(tmp_path.iterdir()) == []

    def test_pyarrow_is_not_installed(self, client, properties, monkeypatch):
        monkeypatch.setitem(sys.modules, "pyarrow", None)
        with pytest.raises(KustoArrowNotInstalledError, match=r"azure-kusto-ingest\[pyarrow\]"):
            client.ingest_from_arrow(sample_table(), properties)

    def test_importing_the_client_does_not_need_pyarrow(self):
        import subprocess

        code = "import sys; sys.modules['pyarrow'] = None; import azure.kusto.ingest, azure.kusto.ingest.base_ingest_client"
        subprocess.run([sys.executable, "-c", code], check=True)
