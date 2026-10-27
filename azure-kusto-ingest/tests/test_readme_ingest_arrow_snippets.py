# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
"""Runs the Python code blocks of the Arrow section of README.rst, so that they keep working."""

import os
import re
import textwrap
import uuid

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from azure.kusto.data.data_format import DataFormat  # noqa: E402
from azure.kusto.ingest import IngestionProperties, IngestionStatus  # noqa: E402
from azure.kusto.ingest.base_ingest_client import BaseIngestClient, IngestionResult  # noqa: E402
from azure.kusto.ingest.descriptors import FileDescriptor  # noqa: E402

README = os.path.join(os.path.dirname(__file__), "..", "README.rst")


def arrow_snippets():
    with open(README, newline="") as f:
        text = f.read().replace("\r\n", "\n")
    section = text[text.index("\nArrow\n~~~~~\n") : text.index("\nLinks\n~~~~~\n")]
    blocks = re.findall(r"^\.\. code-block:: python\n\n((?:    .*\n|\n)+)", section, re.MULTILINE)
    return [textwrap.dedent(block).strip() + "\n" for block in blocks]


class RecordingClient(BaseIngestClient):
    """Records the Parquet files which are ingested, reading them while they exist"""

    def __init__(self):
        super().__init__()
        self.ingestions = []

    def ingest_from_stream(self, stream_descriptor, ingestion_properties):
        raise NotImplementedError

    def set_proxy(self, proxy_url):
        raise NotImplementedError

    def ingest_from_file(self, file_descriptor, ingestion_properties):
        super().ingest_from_file(file_descriptor, ingestion_properties)
        path = FileDescriptor.get_instance(file_descriptor).path
        self.ingestions.append({"format": ingestion_properties.format, "file": pq.ParquetFile(path).metadata, "table": pq.read_table(path)})
        return IngestionResult(IngestionStatus.QUEUED, ingestion_properties.database, ingestion_properties.table, uuid.uuid4())


def test_arrow_snippets():
    snippets = arrow_snippets()
    assert len(snippets) == 4

    client = RecordingClient()
    # The names that the first example of the README imports
    namespace = {"client": client, "IngestionProperties": IngestionProperties, "DataFormat": DataFormat}
    for snippet in snippets:
        exec(compile(snippet, "README.rst", "exec"), namespace)

    assert [ingestion["format"] for ingestion in client.ingestions] == [DataFormat.PARQUET] * 3
    first, nanoseconds, every_tick = client.ingestions
    assert first["table"].column("name").to_pylist() == ["abc", "cde"]
    assert nanoseconds["table"].num_rows == 2

    # The datetime is ingested as integer nanoseconds, together with the mapping which converts it
    assert every_tick["table"].schema.field("when").type == pa.int64()
    assert every_tick["table"].column("when").to_pylist() == [1_700_000_000_123_456_700]
    mapping = namespace["ingestion_props"].ingestion_mapping
    assert [column.column for column in mapping] == ["id", "when"]
    assert mapping[1].properties["Transform"] == "DateTimeFromUnixNanoseconds"
