# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
"""Runs the Python code blocks of the Arrow section of README.rst, so that they keep working."""

import json
import os
import re
import textwrap
from io import BytesIO

import pytest

pa = pytest.importorskip("pyarrow")

from azure.kusto.data.response import KustoResponseDataSetV2, KustoStreamingResponseDataSet  # noqa: E402
from azure.kusto.data.streaming_response import JsonTokenReader, StreamingDataSetEnumerator  # noqa: E402

INPUT_DIR = os.path.join(os.path.dirname(__file__), "input")
README = os.path.join(os.path.dirname(__file__), "..", "README.rst")


def read_input(file_name: str) -> bytes:
    with open(os.path.join(INPUT_DIR, file_name), "rb") as f:
        return f.read()


def arrow_snippets():
    with open(README, newline="") as f:
        text = f.read().replace("\r\n", "\n")
    section = text[text.index("\nArrow\n~~~~~\n") : text.index("\nLinks\n~~~~~\n")]
    blocks = re.findall(r"^\.\. code(?:-block)?:: python\n\n((?:    .*\n|\n)+)", section, re.MULTILINE)
    return [textwrap.dedent(block).strip() + "\n" for block in blocks]


class FakeClient:
    """Answers every query with the recorded response of the deft.json file."""

    def execute(self, database, query):
        return KustoResponseDataSetV2(json.loads(read_input("deft.json")))

    def execute_streaming_query(self, database, query):
        return KustoStreamingResponseDataSet(StreamingDataSetEnumerator(JsonTokenReader(BytesIO(read_input("deft.json")))))


class AsyncFile:
    def __init__(self, data: bytes):
        self.bytes_io = BytesIO(data)

    async def read(self, n=-1):
        return self.bytes_io.read(n)


class FakeAsyncClient:
    async def execute_streaming_query(self, database, query):
        from azure.kusto.data.aio.response import KustoStreamingResponseDataSet as AsyncStreamingResponseDataSet
        from azure.kusto.data.aio.streaming_response import JsonTokenReader as AsyncJsonTokenReader, StreamingDataSetEnumerator as AsyncEnumerator

        return AsyncStreamingResponseDataSet(AsyncEnumerator(AsyncJsonTokenReader(AsyncFile(read_input("deft.json")))))


def test_the_section_has_the_snippets():
    snippets = arrow_snippets()
    assert len(snippets) == 5
    assert sum("await " in snippet for snippet in snippets) == 1


def test_synchronous_snippets():
    namespace = {"client": FakeClient(), "db": "Samples"}
    for snippet in (snippet for snippet in arrow_snippets() if "await " not in snippet):
        exec(compile(snippet, "README.rst", "exec"), namespace)

    # The variables of the last snippets: the time resolution one reads the response of the first
    assert isinstance(namespace["tables"], list) and len(namespace["tables"]) == 1
    assert isinstance(namespace["reader"], pa.RecordBatchReader)
    assert namespace["reader"].read_all().num_rows == 11
    assert namespace["table"].schema.field("xdate").type == pa.timestamp("ns", "UTC")
    assert namespace["table"].num_rows == 11


@pytest.mark.asyncio
async def test_asynchronous_snippet(capsys):
    pytest.importorskip("aiohttp")
    snippet = next(snippet for snippet in arrow_snippets() if "await " in snippet)
    namespace = {"client": FakeAsyncClient(), "db": "Samples"}
    exec(compile("async def snippet():\n" + textwrap.indent(snippet, "    "), "README.rst", "exec"), namespace)

    await namespace["snippet"]()
    assert capsys.readouterr().out.split() == ["11"]
