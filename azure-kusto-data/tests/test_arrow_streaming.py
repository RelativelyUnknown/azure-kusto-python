# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import json
import os
from io import BytesIO

import pytest

pa = pytest.importorskip("pyarrow")

from azure.kusto.data.arrow_helpers import (  # noqa: E402
    arrow_reader_from_result_table,
    arrow_record_batches_from_result_table,
    arrow_table_from_result_table,
    arrow_tables_from_response,
)
from azure.kusto.data.exceptions import KustoMultiApiError, KustoStreamingQueryError  # noqa: E402
from azure.kusto.data.response import KustoStreamingResponseDataSet  # noqa: E402
from azure.kusto.data.streaming_response import JsonTokenReader, StreamingDataSetEnumerator  # noqa: E402
from tests.kusto_client_common import KustoClientTestsMixin  # noqa: E402


def open_input(file_name: str):
    return open(os.path.join(os.path.dirname(__file__), "input", file_name), "rb")


def streaming_response(frames: list) -> KustoStreamingResponseDataSet:
    return KustoStreamingResponseDataSet(StreamingDataSetEnumerator(JsonTokenReader(BytesIO(json.dumps(frames).encode()))))


def primary_table(table_id: int, name: str, rows: list) -> dict:
    return {
        "FrameType": "DataTable",
        "TableId": table_id,
        "TableKind": "PrimaryResult",
        "TableName": name,
        "Columns": [{"ColumnName": "x", "ColumnType": "long"}, {"ColumnName": "t", "ColumnType": "datetime"}],
        "Rows": rows,
    }


def two_primary_tables(rows_per_table: int = 5) -> list:
    def rows(base):
        return [[base + i, "2021-12-22T11:43:0{}.0000001Z".format(i % 10)] for i in range(rows_per_table)]

    return [
        {"FrameType": "DataSetHeader", "IsProgressive": False, "Version": "v2.0"},
        primary_table(0, "First", rows(0)),
        primary_table(1, "Second", rows(100)),
        {"FrameType": "DataSetCompletion", "HasErrors": False, "Cancelled": False},
    ]


class TestStreamingArrow:
    def test_reader(self):
        with open_input("deft.json") as f:
            response = KustoStreamingResponseDataSet(StreamingDataSetEnumerator(JsonTokenReader(f)))
            tables = response.iter_primary_results()
            reader = arrow_reader_from_result_table(next(tables), batch_size=4)
            assert isinstance(reader, pa.RecordBatchReader)
            batches = list(reader)
            assert [batch.num_rows for batch in batches] == [4, 4, 3]
            KustoClientTestsMixin._assert_sanity_arrow_response(pa.Table.from_batches(batches))

            # The table was read entirely, so the response can move on
            assert next(tables, None) is None
            assert response.errors_count == 0
            assert response.finished

    def test_table_is_marked_as_read_only_once_exhausted(self):
        response = streaming_response(two_primary_tables(5))
        tables = response.iter_primary_results()
        first = next(tables)
        batches = arrow_record_batches_from_result_table(first, batch_size=2)

        assert next(batches).num_rows == 2
        assert not first.finished
        with pytest.raises(KustoStreamingQueryError, match="before the old one was finished"):
            next(tables)

        assert [batch.num_rows for batch in batches] == [2, 1]
        assert first.finished and first.rows_count == 5

        second = next(tables)
        assert arrow_table_from_result_table(second).column("x").to_pylist() == [100, 101, 102, 103, 104]

    def test_all_the_tables(self):
        tables = arrow_tables_from_response(streaming_response(two_primary_tables(3)), batch_size=2)
        assert [table.schema.metadata[b"kusto.table_name"] for table in tables] == [b"First", b"Second"]
        assert [table.column("x").to_pylist() for table in tables] == [[0, 1, 2], [100, 101, 102]]
        assert tables[0].schema.field("t").type == pa.timestamp("ns", "UTC")

    def test_reader_with_pyarrow_consumers(self):
        response = streaming_response(two_primary_tables(7))
        reader = arrow_reader_from_result_table(next(response.iter_primary_results()), batch_size=3)
        sink = BytesIO()
        with pa.ipc.new_stream(sink, reader.schema) as writer:
            for batch in reader:
                writer.write_batch(batch)
        assert pa.ipc.open_stream(sink.getvalue()).read_all().num_rows == 7

    def test_error_in_the_middle_of_the_table(self):
        with open_input("query_partial_results_defer_is_false.json") as f:
            response = KustoStreamingResponseDataSet(StreamingDataSetEnumerator(JsonTokenReader(f)))
            table = next(response.iter_primary_results())
            reader = arrow_reader_from_result_table(table, batch_size=2)
            assert reader.read_next_batch().num_rows == 2
            with pytest.raises(KustoMultiApiError) as error:
                reader.read_all()
            assert error.value.get_api_errors()[0].code == "LimitsExceeded"
            assert not table.finished

    def test_error_when_converting_a_table(self):
        with open_input("query_partial_results_defer_is_false.json") as f:
            table = next(KustoStreamingResponseDataSet(StreamingDataSetEnumerator(JsonTokenReader(f))).iter_primary_results())
            with pytest.raises(KustoMultiApiError):
                arrow_table_from_result_table(table)

    def test_conversion_error_is_raised_by_the_reader(self):
        frames = two_primary_tables(4)
        frames[1]["Rows"][3][1] = "not a datetime"
        table = next(streaming_response(frames).iter_primary_results())
        reader = arrow_reader_from_result_table(table, batch_size=2, errors="raise")
        assert reader.read_next_batch().num_rows == 2
        with pytest.raises(ValueError, match="not a datetime"):
            reader.read_next_batch()

    def test_empty_table(self):
        frames = two_primary_tables(0)
        table = next(streaming_response(frames).iter_primary_results())
        reader = arrow_reader_from_result_table(table)
        assert reader.schema.names == ["x", "t"]
        assert reader.read_all().num_rows == 0
        assert table.finished
