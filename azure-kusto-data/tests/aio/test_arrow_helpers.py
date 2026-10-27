# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import json
import os
from io import BytesIO

import pytest

pa = pytest.importorskip("pyarrow")

from azure.kusto.data._models import KustoResultTable, KustoStreamingResultTable as SyncKustoStreamingResultTable  # noqa: E402
from azure.kusto.data.aio._models import KustoStreamingResultTable  # noqa: E402
from azure.kusto.data.aio.arrow_helpers import (  # noqa: E402
    arrow_record_batches_from_streaming_result_table,
    arrow_table_from_streaming_result_table,
    arrow_tables_from_streaming_response,
)
from azure.kusto.data.aio.response import KustoStreamingResponseDataSet  # noqa: E402
from azure.kusto.data.aio.streaming_response import JsonTokenReader, StreamingDataSetEnumerator  # noqa: E402
from azure.kusto.data.arrow_helpers import arrow_table_from_result_table  # noqa: E402
from azure.kusto.data.exceptions import KustoMultiApiError, KustoStreamingQueryError  # noqa: E402
from tests.kusto_client_common import KustoClientTestsMixin  # noqa: E402


class AsyncFile:
    def __init__(self, data: bytes):
        self.bytes_io = BytesIO(data)

    async def read(self, n=-1):
        return self.bytes_io.read(n)


def input_file(file_name: str) -> AsyncFile:
    with open(os.path.join(os.path.dirname(__file__), "..", "input", file_name), "rb") as f:
        return AsyncFile(f.read())


def streaming_response(stream: AsyncFile) -> KustoStreamingResponseDataSet:
    return KustoStreamingResponseDataSet(StreamingDataSetEnumerator(JsonTokenReader(stream)))


def two_primary_tables(rows_per_table: int = 5) -> AsyncFile:
    def table(table_id: int, name: str, base: int) -> dict:
        return {
            "FrameType": "DataTable",
            "TableId": table_id,
            "TableKind": "PrimaryResult",
            "TableName": name,
            "Columns": [{"ColumnName": "x", "ColumnType": "long"}, {"ColumnName": "t", "ColumnType": "timespan"}],
            "Rows": [[base + i, "00:00:0{}".format(i % 10)] for i in range(rows_per_table)],
        }

    frames = [
        {"FrameType": "DataSetHeader", "IsProgressive": False, "Version": "v2.0"},
        table(0, "First", 0),
        table(1, "Second", 100),
        {"FrameType": "DataSetCompletion", "HasErrors": False, "Cancelled": False},
    ]
    return AsyncFile(json.dumps(frames).encode())


class TestAsyncArrow:
    @pytest.mark.asyncio
    async def test_table(self):
        response = streaming_response(input_file("deft.json"))
        tables = response.iter_primary_results()
        first = await tables.__anext__()
        assert isinstance(first, KustoStreamingResultTable)

        table = await arrow_table_from_streaming_result_table(first, batch_size=4)
        KustoClientTestsMixin._assert_sanity_arrow_response(table)
        assert [len(chunk) for chunk in table.column(0).chunks] == [4, 4, 3]
        assert table.schema.metadata[b"kusto.table_name"] == b"Deft"

        # The table was read entirely, so the response can move on
        assert first.finished and first.rows_count == 11
        with pytest.raises(StopAsyncIteration):
            await tables.__anext__()
        assert response.errors_count == 0

    @pytest.mark.asyncio
    async def test_record_batches_are_lazy(self):
        response = streaming_response(two_primary_tables(5))
        tables = response.iter_primary_results()
        first = await tables.__anext__()

        batches = arrow_record_batches_from_streaming_result_table(first, batch_size=2)
        assert first.row_count == 0
        assert (await batches.__anext__()).num_rows == 2
        assert not first.finished
        with pytest.raises(KustoStreamingQueryError, match="before the old one was finished"):
            await tables.__anext__()

        assert [batch.num_rows async for batch in batches] == [2, 1]
        assert first.finished and first.rows_count == 5

        second = await tables.__anext__()
        assert (await arrow_table_from_streaming_result_table(second)).column("x").to_pylist() == [100, 101, 102, 103, 104]

    @pytest.mark.asyncio
    async def test_options(self):
        first = await streaming_response(two_primary_tables(3)).iter_primary_results().__anext__()
        table = await arrow_table_from_streaming_result_table(first, time_unit="us", converters_by_column_name={"x": pa.int8()})
        assert [field.type for field in table.schema] == [pa.int8(), pa.duration("us")]

    @pytest.mark.asyncio
    async def test_all_the_primary_tables(self):
        tables = await arrow_tables_from_streaming_response(streaming_response(two_primary_tables(3)), batch_size=2)
        assert [table.schema.metadata[b"kusto.table_name"] for table in tables] == [b"First", b"Second"]
        assert [table.column("x").to_pylist() for table in tables] == [[0, 1, 2], [100, 101, 102]]

    @pytest.mark.asyncio
    async def test_all_the_tables(self):
        tables = await arrow_tables_from_streaming_response(streaming_response(input_file("deft.json")), primary_only=False)
        assert [table.schema.metadata[b"kusto.table_kind"] for table in tables] == [b"QueryProperties", b"PrimaryResult", b"QueryCompletionInformation"]
        KustoClientTestsMixin._assert_sanity_arrow_response(tables[1])
        assert tables[2].num_rows == 2

    @pytest.mark.asyncio
    async def test_not_streaming_table(self):
        # The tables which are not primary results are read into memory by the response
        properties = await streaming_response(input_file("deft.json")).__anext__()
        assert isinstance(properties, KustoResultTable)
        converted = await arrow_table_from_streaming_result_table(properties)
        assert converted.equals(arrow_table_from_result_table(properties))
        assert converted.column("Key").to_pylist() == ["Visualization"]
        assert [batch.num_rows async for batch in arrow_record_batches_from_streaming_result_table(properties, batch_size=1)] == [1]

    @pytest.mark.asyncio
    async def test_error_in_the_middle_of_the_table(self):
        response = streaming_response(input_file("query_partial_results_defer_is_false.json"))
        table = await response.iter_primary_results().__anext__()
        batches = arrow_record_batches_from_streaming_result_table(table, batch_size=2)
        assert (await batches.__anext__()).num_rows == 2
        with pytest.raises(KustoMultiApiError) as error:
            async for _ in batches:
                pass
        assert error.value.get_api_errors()[0].code == "LimitsExceeded"

    @pytest.mark.asyncio
    async def test_validation(self):
        first = await streaming_response(two_primary_tables(1)).iter_primary_results().__anext__()
        with pytest.raises(ValueError, match="batch_size"):
            arrow_record_batches_from_streaming_result_table(first, batch_size=0)
        with pytest.raises(ValueError):
            await arrow_table_from_streaming_result_table(None)
        with pytest.raises(TypeError, match="Expected"):
            await arrow_table_from_streaming_result_table("table")
        with pytest.raises(TypeError, match="asynchronous KustoStreamingResponseDataSet"):
            await arrow_tables_from_streaming_response([])

    @pytest.mark.asyncio
    async def test_sync_and_async_streaming_tables_are_not_interchangeable(self):
        first = await streaming_response(two_primary_tables(1)).iter_primary_results().__anext__()
        with pytest.raises(TypeError, match="azure.kusto.data.aio.arrow_helpers"):
            arrow_table_from_result_table(first)

        sync_table = SyncKustoStreamingResultTable({"TableName": "T", "Columns": [{"ColumnName": "c", "ColumnType": "long"}], "Rows": iter([[1]])})
        with pytest.raises(TypeError, match="azure.kusto.data.arrow_helpers"):
            await arrow_table_from_streaming_result_table(sync_table)
