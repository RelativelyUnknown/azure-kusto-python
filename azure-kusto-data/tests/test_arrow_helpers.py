# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import calendar
import json
import math
import os
import subprocess
import sys
import uuid
from decimal import Decimal

import pandas
import pytest

pa = pytest.importorskip("pyarrow")

from azure.kusto.data._models import KustoResultTable, KustoStreamingResultTable, WellKnownDataSet  # noqa: E402
from azure.kusto.data.arrow_helpers import (  # noqa: E402
    KUSTO_TABLE_ID_METADATA_KEY,
    KUSTO_TABLE_KIND_METADATA_KEY,
    KUSTO_TABLE_NAME_METADATA_KEY,
    KUSTO_TYPE_METADATA_KEY,
    arrow_reader_from_result_table,
    arrow_record_batches_from_result_table,
    arrow_schema_from_result_table,
    arrow_table_from_result_table,
    arrow_tables_from_response,
)
from azure.kusto.data.exceptions import KustoArrowNotInstalledError  # noqa: E402
from azure.kusto.data.helpers import dataframe_from_result_table  # noqa: E402
from azure.kusto.data.response import KustoResponseDataSetV1, KustoResponseDataSetV2  # noqa: E402
from tests.kusto_client_common import KustoClientTestsMixin  # noqa: E402


def load_response(file_name: str, v1: bool = False):
    with open(os.path.join(os.path.dirname(__file__), "input", file_name), "r") as response_file:
        data = json.load(response_file)
    return KustoResponseDataSetV1(data) if v1 else KustoResponseDataSetV2(data)


def make_table(column_type: str, rows: list) -> KustoResultTable:
    return KustoResultTable({"TableName": "T", "Columns": [{"ColumnName": "c", "ColumnType": column_type}], "Rows": [[row] for row in rows]})


def convert(column_type: str, rows: list, **kwargs) -> "pa.Array":
    return arrow_table_from_result_table(make_table(column_type, rows), **kwargs).column("c").combine_chunks()


def as_ints(array: "pa.Array") -> list:
    """The raw values of a temporal array, as python's datetime can't represent all of them"""
    return array.cast(pa.int64()).to_pylist()


def utc_seconds(year, month=1, day=1, hour=0, minute=0, second=0) -> int:
    return calendar.timegm((year, month, day, hour, minute, second, 0, 0, 0))


class TestTypeMapping:
    @pytest.mark.parametrize(
        "column_type, expected, kusto_type",
        [
            ("bool", pa.bool_(), "bool"),
            ("boolean", pa.bool_(), "bool"),
            ("int", pa.int32(), "int"),
            ("int32", pa.int32(), "int"),
            ("long", pa.int64(), "long"),
            ("int64", pa.int64(), "long"),
            ("real", pa.float64(), "real"),
            ("double", pa.float64(), "real"),
            ("decimal", pa.float64(), "decimal"),
            ("string", pa.string(), "string"),
            ("guid", pa.string(), "guid"),
            ("uuid", pa.string(), "guid"),
            ("uniqueid", pa.string(), "guid"),
            ("datetime", pa.timestamp("us", "UTC"), "datetime"),
            ("date", pa.timestamp("us", "UTC"), "datetime"),
            ("timespan", pa.duration("us"), "timespan"),
            ("time", pa.duration("us"), "timespan"),
            ("dynamic", pa.string(), "dynamic"),
            # The .NET names, which the management API reports
            ("String", pa.string(), "string"),
            ("Int32", pa.int32(), "int"),
            ("Int64", pa.int64(), "long"),
            ("DateTime", pa.timestamp("us", "UTC"), "datetime"),
            ("TimeSpan", pa.duration("us"), "timespan"),
            ("Guid", pa.string(), "guid"),
            ("SByte", pa.bool_(), "bool"),
            ("Double", pa.float64(), "real"),
            ("Object", pa.string(), "dynamic"),
            ("SqlDecimal", pa.float64(), "decimal"),
        ],
    )
    def test_default_types(self, column_type, expected, kusto_type):
        schema = arrow_schema_from_result_table(make_table(column_type, []))
        assert schema.field("c").type == expected
        assert schema.field("c").metadata == {KUSTO_TYPE_METADATA_KEY.encode(): kusto_type.encode()}

    def test_nanosecond_types(self):
        table = KustoResultTable(
            {"TableName": "T", "Columns": [{"ColumnName": "dt", "ColumnType": "datetime"}, {"ColumnName": "ts", "ColumnType": "timespan"}], "Rows": []}
        )
        assert [field.type for field in arrow_schema_from_result_table(table, time_unit="ns")] == [pa.timestamp("ns", "UTC"), pa.duration("ns")]

    def test_options_change_types(self):
        table = KustoResultTable(
            {
                "TableName": "T",
                "Columns": [{"ColumnName": "dt", "ColumnType": "datetime"}, {"ColumnName": "ts", "ColumnType": "timespan"}]
                + [{"ColumnName": "dec", "ColumnType": "decimal"}, {"ColumnName": "g", "ColumnType": "guid"}],
                "Rows": [],
            }
        )
        schema = arrow_schema_from_result_table(table, time_unit="ns", decimal_as="decimal128", decimal_precision=20, decimal_scale=4, guid_as="uuid")
        assert [field.type for field in schema] == [pa.timestamp("ns", "UTC"), pa.duration("ns"), pa.decimal128(20, 4), pa.uuid()]
        assert arrow_schema_from_result_table(table, decimal_as="string").field("dec").type == pa.string()

    def test_unknown_type(self):
        with pytest.raises(ValueError, match="missing"):
            arrow_schema_from_result_table(make_table("missing", []))

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"time_unit": "ms"},
            {"decimal_as": "int"},
            {"guid_as": "bytes"},
            {"errors": "ignore"},
            {"decimal_precision": 39},
            {"decimal_precision": 5, "decimal_scale": 6},
        ],
    )
    def test_invalid_options(self, kwargs):
        with pytest.raises(ValueError):
            arrow_schema_from_result_table(make_table("string", []), **kwargs)


class TestTable:
    def test_sanity(self):
        table = arrow_table_from_result_table(load_response("deft.json").primary_results[0])
        KustoClientTestsMixin._assert_sanity_arrow_response(table)

    def test_sanity_nanoseconds(self):
        table = arrow_table_from_result_table(load_response("deft.json").primary_results[0], time_unit="ns")
        KustoClientTestsMixin._assert_sanity_arrow_response(table, "ns")

    def test_metadata(self):
        table = arrow_table_from_result_table(load_response("deft.json").primary_results[0])
        assert table.schema.metadata == {
            KUSTO_TABLE_NAME_METADATA_KEY.encode(): b"Deft",
            KUSTO_TABLE_ID_METADATA_KEY.encode(): b"1",
            KUSTO_TABLE_KIND_METADATA_KEY.encode(): WellKnownDataSet.PrimaryResult.value.encode(),
        }
        assert table.schema.field("xdynamicWithNulls").metadata == {b"kusto.type": b"dynamic"}

    def test_schema_matches_table(self):
        source = load_response("deft.json").primary_results[0]
        assert arrow_schema_from_result_table(source).equals(arrow_table_from_result_table(source).schema, check_metadata=True)

    def test_matches_pandas(self):
        source = load_response("deft.json").primary_results[0]
        arrow_frame = arrow_table_from_result_table(source, time_unit="ns").to_pandas()  # pandas converts to nanoseconds
        pandas_frame = dataframe_from_result_table(source)
        # xtime is not compared: the pandas conversion reads a negative timespan with days, like "-2.00:00:02", as -2 days and +2 seconds
        for column in ["rownumber", "xdouble", "xint64", "xdate", "xsmalltext"]:
            for arrow_value, pandas_value in zip(arrow_frame[column], pandas_frame[column]):
                if pandas.isna(arrow_value):
                    assert pandas.isna(pandas_value), column
                else:
                    assert arrow_value == pandas_value, column

    def test_special_values_nanoseconds(self):
        source = load_response("dataframe.json").primary_results[0]
        table = arrow_table_from_result_table(source, time_unit="ns", converters_by_column_name={"MissingType": pa.string()})
        assert table.num_rows == 8
        real = table.column("RecordReal").to_pylist()
        assert real[0] == 3.14159
        assert math.isnan(real[1])  # NaN is not null
        assert real[2] == math.inf and real[3] == -math.inf
        assert real[7] is None
        assert table.column("RecordDecimal").to_pylist()[2] == math.inf
        # Kusto's datetime(0001-01-01) and datetime(9999-12-31) can't be represented as nanoseconds
        times = as_ints(table.column("RecordTime").combine_chunks())
        assert times[1] is None and times[2] is None
        assert times[0] == utc_seconds(2021, 12, 22, 11, 43) * 10**9
        assert times[3] == utc_seconds(1677, 9, 21, 0, 12, 44) * 10**9  # The earliest second pandas can represent
        assert times[4] == utc_seconds(2262, 4, 11, 23, 47, 16) * 10**9
        assert times[7] is None
        # Timespans, either as a number of ticks or as a string
        assert as_ints(table.column("RecordOffset").combine_chunks()) == [0, 0, 0, 0, 0, 60 * 10**9, (24 * 3600 + 3661) * 10**9, 0]
        # A row which is shorter than the number of columns is padded with nulls
        assert table.column("RecordName").to_pylist()[7] is None
        assert table.column("RecordBool").to_pylist() == [True] * 7 + [False]
        assert table.column("RecordGUID").to_pylist()[7] is None

    def test_special_values(self):
        source = load_response("dataframe.json").primary_results[0]
        table = arrow_table_from_result_table(source, converters_by_column_name={"MissingType": pa.string()})
        assert table.schema.field("RecordTime").type == pa.timestamp("us", "UTC")
        real = table.column("RecordReal").to_pylist()
        assert real[0] == 3.14159 and math.isnan(real[1]) and real[2:4] == [math.inf, -math.inf] and real[7] is None
        times = as_ints(table.column("RecordTime").combine_chunks())
        # Unlike with nanoseconds, the earliest and latest datetimes of Kusto are not lost
        assert times[1] == -62167219200 * 10**6  # The fixture's "0000-01-01" is accepted as well
        assert times[2] == utc_seconds(9999, 12, 31, 23, 59, 59) * 10**6
        assert times[0] == utc_seconds(2021, 12, 22, 11, 43) * 10**6
        assert times[7] is None
        assert as_ints(table.column("RecordOffset").combine_chunks())[6] == (24 * 3600 + 3661) * 10**6

    def test_null_values(self):
        table = arrow_table_from_result_table(load_response("null_values.json").primary_results[0])
        assert table.num_rows == 1
        assert [table.column(i).to_pylist() for i in range(table.num_columns)] == [[None]] * 10

    def test_dynamic(self):
        table = arrow_table_from_result_table(load_response("dynamic.json").primary_results[0])
        assert [column.to_pylist()[0] for column in table.columns] == [
            "123",
            "123",
            "test bad json",
            None,
            '{"rowId":2,"arr":[0,2]}',
            '{"rowId":2,"arr":[0,2]}',
        ]

    def test_dynamic_values(self):
        values = [None, 1, 2.5, True, "text", {"a": [1, None], "b": "é"}, [1, 2]]
        assert convert("dynamic", values).to_pylist() == [None, "1", "2.5", "true", "text", '{"a":[1,null],"b":"é"}', "[1,2]"]

    def test_empty_table(self):
        table = arrow_table_from_result_table(load_response("zero_results.json").primary_results[0])
        assert table.num_rows == 0
        assert table.schema.names == ["print_0"]
        assert table.schema.field("print_0").type == pa.string()

    def test_mgmt_response(self):
        response = load_response("adminthenquery.json", v1=True)
        tables = arrow_tables_from_response(response)
        assert len(tables) == 1
        assert tables[0].schema.names == ["DatabaseName", "TableName"]
        assert tables[0].column("TableName").to_pylist() == [t[1] for t in response.primary_results[0].raw_rows]

        info = arrow_tables_from_response(response, primary_only=False)[2]
        assert [field.type for field in info.schema] == [
            pa.timestamp("us", "UTC"),
            pa.int32(),
            pa.string(),
            pa.int32(),
            pa.string(),
            pa.int32(),
            pa.string(),
            pa.string(),
            pa.string(),
            pa.string(),
        ]

    def test_version_command(self):
        table = arrow_table_from_result_table(load_response("versionshowcommandresult.json", v1=True).primary_results[0])
        assert table.column("BuildVersion").to_pylist() == ["1.0.6693.14577"]
        assert as_ints(table.column("BuildTime").combine_chunks()) == [utc_seconds(2018, 4, 29, 8, 5, 54) * 10**6]

    def test_batch_size(self):
        table = make_table("long", list(range(10)))
        result = arrow_table_from_result_table(table, batch_size=4)
        assert [len(chunk) for chunk in result.column("c").chunks] == [4, 4, 2]
        assert result.column("c").to_pylist() == list(range(10))
        with pytest.raises(ValueError):
            arrow_table_from_result_table(table, batch_size=0)

    def test_streaming_table(self):
        columns = [{"ColumnName": "c", "ColumnType": "long"}]
        streaming = KustoStreamingResultTable({"TableName": "T", "TableKind": "PrimaryResult", "Columns": columns, "Rows": iter([[i] for i in range(5)])})
        result = arrow_table_from_result_table(streaming, batch_size=2)
        assert result.column("c").to_pylist() == [0, 1, 2, 3, 4]
        # The streaming table is marked as read
        assert streaming.finished
        assert streaming.rows_count == 5

    def test_streaming_table_is_single_pass(self):
        columns = [{"ColumnName": "c", "ColumnType": "long"}]
        streaming = KustoStreamingResultTable({"TableName": "T", "Columns": columns, "Rows": iter([[1], [2]])})
        assert arrow_table_from_result_table(streaming).num_rows == 2
        assert arrow_table_from_result_table(streaming).num_rows == 0

    @pytest.mark.parametrize("bad", [None, 5, "table", []])
    def test_invalid_input(self, bad):
        with pytest.raises((ValueError, TypeError)):
            arrow_table_from_result_table(bad)

    def test_columnless_table(self):
        with pytest.raises(ValueError):
            arrow_table_from_result_table(KustoResultTable({"TableName": "T", "Columns": [], "Rows": []}))

    def test_wrong_table_type(self):
        with pytest.raises(TypeError, match="KustoResultTable"):
            arrow_table_from_result_table(load_response("deft.json"))

    def test_ragged_rows(self):
        table = KustoResultTable(
            {
                "TableName": "T",
                "Columns": [{"ColumnName": "a", "ColumnType": "long"}, {"ColumnName": "b", "ColumnType": "string"}],
                "Rows": [[1, "x"], [2], [3, "z"]],
            }
        )
        result = arrow_table_from_result_table(table)
        assert result.column("a").to_pylist() == [1, 2, 3]
        assert result.column("b").to_pylist() == ["x", None, "z"]
        too_long = KustoResultTable({"TableName": "T", "Columns": [{"ColumnName": "a", "ColumnType": "long"}], "Rows": [[1, 2]]})
        with pytest.raises(ValueError, match="2 values"):
            arrow_table_from_result_table(too_long)


class TestValues:
    def test_bool(self):
        assert convert("bool", [True, None, False]).to_pylist() == [True, None, False]
        assert convert("bool", [1, 0, "true", "False", "x", 2.5, None]).to_pylist() == [True, False, True, False, None, True, None]
        with pytest.raises(ValueError, match="'x'"):
            convert("bool", ["x"], errors="raise")

    def test_integers(self):
        assert convert("int", [1, None, -2]).to_pylist() == [1, None, -2]
        assert convert("long", [2**62, -(2**62)]).to_pylist() == [2**62, -(2**62)]
        # Values which can't be converted become nulls
        assert convert("int", ["12", 3.0, 3.5, 2**40, "x", True]).to_pylist() == [12, 3, None, None, None, 1]
        assert convert("long", [2**70, 5]).to_pylist() == [None, 5]
        with pytest.raises(ValueError, match="out of the range"):
            convert("int", [2**40], errors="raise")

    def test_real(self):
        values = convert("real", [1, 2.5, None, "NaN", "Infinity", "-Infinity", "3.5", "bad"]).to_pylist()
        assert values[:3] == [1.0, 2.5, None]
        assert math.isnan(values[3])
        assert values[4:] == [math.inf, -math.inf, 3.5, None]
        with pytest.raises(ValueError, match="'bad'"):
            convert("real", ["bad"], errors="raise")

    def test_real_null_is_not_nan(self):
        values = convert("real", [None, "NaN"]).to_pylist()
        assert values[0] is None and math.isnan(values[1])

    def test_string(self):
        assert convert("string", ["a", "", None, "é"]).to_pylist() == ["a", "", None, "é"]
        assert convert("string", ["a", 1, True]).to_pylist() == ["a", "1", "True"]

    def test_decimal(self):
        assert convert("decimal", [1.5, "2.25", None, "NaN"]).to_pylist()[:3] == [1.5, 2.25, None]
        result = convert("decimal", ["1.2", 3, 2.5, Decimal("0.1"), None, "NaN", "1e40"], decimal_as="decimal128", decimal_precision=20, decimal_scale=4)
        assert result.type == pa.decimal128(20, 4)
        assert result.to_pylist() == [Decimal("1.2000"), Decimal("3.0000"), Decimal("2.5000"), Decimal("0.1000"), None, None, None]
        assert convert("decimal", ["1.234567890123456789012345", 5], decimal_as="string").to_pylist() == ["1.234567890123456789012345", "5"]
        with pytest.raises(ValueError, match="1e40"):
            convert("decimal", ["1e40"], decimal_as="decimal128", decimal_precision=20, decimal_scale=4, errors="raise")

    def test_guid(self):
        guid = "6f3c1072-2739-461c-8aa7-3cfc8ff528a8"
        assert convert("guid", [guid, None, ""]).to_pylist() == [guid, None, ""]
        result = convert("guid", [guid, None, "", "6F3C10722739461C8AA73CFC8FF528A8"], guid_as="uuid")
        assert result.type == pa.uuid()
        assert result.to_pylist() == [uuid.UUID(guid), None, None, uuid.UUID(guid)]
        assert convert("guid", ["not a guid"], guid_as="uuid").to_pylist() == [None]
        with pytest.raises(ValueError, match="not a guid"):
            convert("guid", ["not a guid"], guid_as="uuid", errors="raise")


class TestDatetime:
    @pytest.mark.parametrize(
        "value, seconds, ticks",
        [
            ("2014-01-01T01:01:01.0000000Z", utc_seconds(2014, 1, 1, 1, 1, 1), 0),
            ("2015-01-01T01:01:01.0000001Z", utc_seconds(2015, 1, 1, 1, 1, 1), 1),
            ("2021-12-22T11:43:00Z", utc_seconds(2021, 12, 22, 11, 43), 0),
            ("2023-12-12T01:59:59.352Z", utc_seconds(2023, 12, 12, 1, 59, 59), 3520000),
            ("2023-12-12T01:59:59.123456789Z", utc_seconds(2023, 12, 12, 1, 59, 59), 1234567),  # Digits beyond the ticks are dropped
            ("1960-01-01T00:00:00.5Z", utc_seconds(1960), 5000000),
            ("1970-01-01T00:00:00Z", 0, 0),
            ("1677-09-21T00:12:44Z", utc_seconds(1677, 9, 21, 0, 12, 44), 0),
            # The earliest and the latest moments which fit in 64 bits of nanoseconds
            ("1677-09-21T00:12:43.1452242Z", utc_seconds(1677, 9, 21, 0, 12, 43), 1452242),
            ("2262-04-11T23:47:16.8547758Z", utc_seconds(2262, 4, 11, 23, 47, 16), 8547758),
        ],
    )
    def test_nanoseconds(self, value, seconds, ticks):
        result = convert("datetime", [value], time_unit="ns")
        assert result.type == pa.timestamp("ns", "UTC")
        assert as_ints(result) == [seconds * 10**9 + ticks * 100]

    @pytest.mark.parametrize(
        "value",
        ["0001-01-01T00:00:00Z", "9999-12-31T23:59:59.9999999Z", "1677-09-21T00:12:43.1452241Z", "2262-04-11T23:47:16.8547759Z", "2262-04-11T23:47:17Z"],
    )
    def test_nanoseconds_out_of_range(self, value):
        assert convert("datetime", [value], time_unit="ns").to_pylist() == [None]
        with pytest.raises(ValueError, match="time_unit='us'"):
            convert("datetime", [value], time_unit="ns", errors="raise")

    @pytest.mark.parametrize(
        "value, microseconds",
        [
            ("2015-01-01T01:01:01.0000001Z", utc_seconds(2015, 1, 1, 1, 1, 1) * 10**6),
            ("2015-01-01T01:01:01.1234567Z", utc_seconds(2015, 1, 1, 1, 1, 1) * 10**6 + 123456),
            ("1960-01-01T00:00:00.1234567Z", utc_seconds(1960) * 10**6 + 123456),  # Rounded down, as is everything before 1970
            ("0001-01-01T00:00:00Z", utc_seconds(1) * 10**6),
            ("9999-12-31T23:59:59.9999999Z", utc_seconds(9999, 12, 31, 23, 59, 59) * 10**6 + 999999),
        ],
    )
    def test_microseconds(self, value, microseconds):
        result = convert("datetime", [value])  # The default
        assert result.type == pa.timestamp("us", "UTC")
        assert as_ints(result) == [microseconds]
        assert as_ints(convert("datetime", [value], time_unit="us")) == [microseconds]

    @pytest.mark.parametrize("unit, per_second", [("us", 10**6), ("ns", 10**9)])
    def test_nulls_and_garbage(self, unit, per_second):
        values = [None, "", "garbage", "2021-12-22T11:43:00Z"]
        assert as_ints(convert("datetime", values, time_unit=unit)) == [None, None, None, utc_seconds(2021, 12, 22, 11, 43) * per_second]
        with pytest.raises(ValueError, match="garbage"):
            convert("datetime", ["garbage"], time_unit=unit, errors="raise")
        with pytest.raises(ValueError, match="column 'c'"):
            convert("datetime", [""], time_unit=unit, errors="raise")

    @pytest.mark.parametrize("unit, per_second", [("us", 10**6), ("ns", 10**9)])
    def test_other_formats(self, unit, per_second):
        values = ["2021-12-22 11:43:00", "2021-12-22T11:43:00+01:00", "2021-12-22T11:43:00.5-02:30", "20211222T114300Z"]
        expected = [
            utc_seconds(2021, 12, 22, 11, 43) * per_second,
            utc_seconds(2021, 12, 22, 10, 43) * per_second,
            utc_seconds(2021, 12, 22, 14, 13) * per_second + per_second // 2,
            utc_seconds(2021, 12, 22, 11, 43) * per_second,
        ]
        assert as_ints(convert("datetime", values, time_unit=unit)) == expected

    @pytest.mark.parametrize("unit", ["us", "ns"])
    def test_all_null_column(self, unit):
        result = convert("datetime", [None, None], time_unit=unit)
        assert result.type == pa.timestamp(unit, "UTC")
        assert result.null_count == 2
        assert convert("datetime", [], time_unit=unit).type == pa.timestamp(unit, "UTC")

    @pytest.mark.parametrize("unit, per_second", [("us", 10**6), ("ns", 10**9)])
    def test_non_string_values(self, unit, per_second):
        assert as_ints(convert("datetime", [None, 12.5, True], time_unit=unit)) == [None, None, None]
        # An ISO 8601 basic format date
        assert as_ints(convert("datetime", [20211222], time_unit=unit)) == [utc_seconds(2021, 12, 22) * per_second]


class TestTimespan:
    @pytest.mark.parametrize(
        "value, ticks",
        [
            ("00:00:00", 0),
            ("00:01:00", 60 * 10**7),
            ("1.00:00:01.0010001", (24 * 3600 + 1) * 10**7 + 10001),
            ("-2.00:00:02.0020002", -((2 * 24 * 3600 + 2) * 10**7 + 20002)),
            ("3.01:01:01", (3 * 24 * 3600 + 3661) * 10**7),
            ("00:00:00.5", 5 * 10**6),
            ("-00:00:00.0000001", -1),
            ("10675199.02:48:05.4775807", 9223372036854775807),
            (600000000, 600000000),
            (600000000.0, 600000000),
            (-5, -5),
        ],
    )
    def test_ticks(self, value, ticks):
        nanoseconds = convert("timespan", [value], time_unit="ns")
        assert nanoseconds.type == pa.duration("ns")
        assert as_ints(nanoseconds) == [ticks * 100 if abs(ticks) <= (2**63 - 1) // 100 else None]

        microseconds = convert("timespan", [value], time_unit="us")
        assert microseconds.type == pa.duration("us")
        # Truncated towards zero
        assert as_ints(microseconds) == [(abs(ticks) // 10) * (-1 if ticks < 0 else 1)]

    @pytest.mark.parametrize("unit, per_second", [("us", 10**6), ("ns", 10**9)])
    def test_nulls_and_garbage(self, unit, per_second):
        assert as_ints(convert("timespan", [None, "", "garbage", "1:2:3", True, "00:00:01"], time_unit=unit)) == [None, None, None, None, None, per_second]
        with pytest.raises(ValueError, match="garbage"):
            convert("timespan", ["garbage"], time_unit=unit, errors="raise")

    def test_out_of_range_nanoseconds(self):
        value = "106752.00:00:00"  # 64 bits of nanoseconds hold up to 106751.99 days
        assert convert("timespan", [value], time_unit="ns").to_pylist() == [None]
        assert as_ints(convert("timespan", [value])) == [106752 * 24 * 3600 * 10**6]  # Microseconds are the default
        with pytest.raises(ValueError, match="nanosecond"):
            convert("timespan", [value], time_unit="ns", errors="raise")

    def test_absurd_numbers(self):
        absurd = [10**30, -(10**30), 1e30, float("inf")]
        for unit, five_ticks in (("ns", 500), ("us", 0)):
            assert as_ints(convert("timespan", absurd + [5], time_unit=unit)) == [None, None, None, None, five_ticks]
        with pytest.raises(ValueError, match="microsecond"):
            convert("timespan", [10**30], time_unit="us", errors="raise")

    def test_boundaries(self):
        limit = (2**63 - 1) // 100
        assert as_ints(convert("timespan", [limit, -limit, limit + 1], time_unit="ns")) == [limit * 100, -limit * 100, None]


class TestConverters:
    def source(self):
        return KustoResultTable(
            {
                "TableName": "T",
                "Columns": [{"ColumnName": "a", "ColumnType": "int"}, {"ColumnName": "b", "ColumnType": "int"}, {"ColumnName": "c", "ColumnType": "string"}],
                "Rows": [[1, 10, "x"], [2, 20, "y"]],
            }
        )

    def test_by_type_and_name(self):
        result = arrow_table_from_result_table(
            self.source(), converters_by_type={"int": pa.int16()}, converters_by_column_name={"b": pa.float32(), "c": pa.large_string()}
        )
        assert [field.type for field in result.schema] == [pa.int16(), pa.float32(), pa.large_string()]
        assert result.column("b").to_pylist() == [10.0, 20.0]
        # The Kusto type is still recorded
        assert result.schema.field("a").metadata == {b"kusto.type": b"int"}

    def test_by_canonical_type(self):
        table = make_table("int32", [1])
        assert arrow_table_from_result_table(table, converters_by_type={"int": pa.int8()}).schema.field("c").type == pa.int8()
        assert arrow_table_from_result_table(table, converters_by_type={"int32": pa.int16(), "int": pa.int8()}).schema.field("c").type == pa.int16()

    def test_name_beats_type(self):
        result = arrow_table_from_result_table(self.source(), converters_by_type={"int": pa.int16()}, converters_by_column_name={"a": pa.int8()})
        assert [field.type for field in result.schema][:2] == [pa.int8(), pa.int16()]

    def test_function(self):
        calls = []

        def to_upper(name, values):
            calls.append((name, tuple(values)))
            return pa.array([value.upper() for value in values], type=pa.large_string())

        result = arrow_table_from_result_table(self.source(), converters_by_column_name={"c": to_upper})
        assert calls == [("c", ("x", "y"))]
        assert result.column("c").to_pylist() == ["X", "Y"]
        assert result.schema.field("c").type == pa.large_string()
        assert result.schema.field("c").metadata == {b"kusto.type": b"string"}

    def test_function_returning_chunked_array(self):
        result = arrow_table_from_result_table(self.source(), converters_by_column_name={"c": lambda name, values: pa.chunked_array([pa.array(list(values))])})
        assert result.column("c").to_pylist() == ["x", "y"]

    def test_function_on_empty_table(self):
        table = make_table("string", [])
        result = arrow_table_from_result_table(table, converters_by_column_name={"c": lambda name, values: pa.array([], type=pa.large_string())})
        assert result.num_rows == 0
        assert result.schema.field("c").type == pa.large_string()

    def test_function_returning_wrong_things(self):
        with pytest.raises(TypeError, match="Arrow array"):
            arrow_table_from_result_table(self.source(), converters_by_column_name={"c": lambda name, values: list(values)})
        with pytest.raises(ValueError, match="1 values for 2 rows"):
            arrow_table_from_result_table(self.source(), converters_by_column_name={"c": lambda name, values: pa.array(["x"])})

    def test_function_changing_type(self):
        def changing(name, values):
            return pa.array(list(values), type=pa.string() if changing.first else pa.large_string())

        changing.first = True

        def convert_and_flip(name, values):
            result = changing(name, values)
            changing.first = False
            return result

        with pytest.raises(ValueError, match="previously returned"):
            arrow_table_from_result_table(make_table("string", ["a", "b", "c"]), batch_size=2, converters_by_column_name={"c": convert_and_flip})

    def test_function_makes_schema_unknown(self):
        with pytest.raises(ValueError, match="function"):
            arrow_schema_from_result_table(self.source(), converters_by_column_name={"c": lambda name, values: pa.array(list(values))})
        assert arrow_schema_from_result_table(self.source(), converters_by_column_name={"c": pa.large_string()}).field("c").type == pa.large_string()

    def test_missing_type_with_converter(self):
        table = KustoResultTable({"TableName": "T", "Columns": [{"ColumnName": "a", "ColumnType": "missing"}], "Rows": [["x"]]})
        result = arrow_table_from_result_table(table, converters_by_column_name={"a": pa.string()})
        assert result.schema.field("a").metadata == {b"kusto.type": b"missing"}
        result = arrow_table_from_result_table(table, converters_by_type={"missing": pa.string()})
        assert result.column("a").to_pylist() == ["x"]

    def test_invalid_converter(self):
        with pytest.raises(TypeError, match="converter"):
            arrow_table_from_result_table(self.source(), converters_by_column_name={"a": "int16"})  # pyright: ignore[reportArgumentType]


class TestResponse:
    def test_primary_only(self):
        tables = arrow_tables_from_response(load_response("deft.json"))
        assert len(tables) == 1
        KustoClientTestsMixin._assert_sanity_arrow_response(tables[0])

    def test_all_tables(self):
        tables = arrow_tables_from_response(load_response("deft.json"), primary_only=False)
        assert [table.schema.metadata[KUSTO_TABLE_KIND_METADATA_KEY.encode()] for table in tables] == [
            b"QueryProperties",
            b"PrimaryResult",
            b"QueryCompletionInformation",
        ]
        assert tables[0].column("Key").to_pylist() == ["Visualization"]
        assert tables[2].num_rows == 2

    def test_options_are_passed(self):
        tables = arrow_tables_from_response(load_response("deft.json"), time_unit="ns", batch_size=5)
        assert tables[0].schema.field("xdate").type == pa.timestamp("ns", "UTC")
        assert [len(chunk) for chunk in tables[0].column("xdate").chunks] == [5, 5, 1]

    def test_invalid_response(self):
        with pytest.raises(TypeError, match="KustoResponseDataSet"):
            arrow_tables_from_response(load_response("deft.json").primary_results[0])


class TestBatches:
    def test_record_batches(self):
        table = make_table("long", list(range(10)))
        batches = arrow_record_batches_from_result_table(table, batch_size=4)
        assert [batch.num_rows for batch in batches] == [4, 4, 2]

    def test_record_batches_are_lazy(self):
        consumed = []

        def rows():
            for i in range(6):
                consumed.append(i)
                yield [i]

        streaming = KustoStreamingResultTable({"TableName": "T", "Columns": [{"ColumnName": "c", "ColumnType": "long"}], "Rows": rows()})
        batches = arrow_record_batches_from_result_table(streaming, batch_size=2)
        assert consumed == []
        first = next(batches)
        assert first.column(0).to_pylist() == [0, 1]
        assert consumed == [0, 1]
        assert not streaming.finished and streaming.row_count == 2
        assert [batch.num_rows for batch in batches] == [2, 2]
        assert streaming.finished and streaming.rows_count == 6

    def test_empty_table_yields_no_batches(self):
        assert list(arrow_record_batches_from_result_table(make_table("long", []))) == []

    def test_reader(self):
        reader = arrow_reader_from_result_table(load_response("deft.json").primary_results[0], batch_size=4)
        assert reader.schema.names[0] == "rownumber"
        table = reader.read_all()
        KustoClientTestsMixin._assert_sanity_arrow_response(table)
        assert [len(chunk) for chunk in table.column(0).chunks] == [4, 4, 3]

    def test_reader_schema_is_known_before_reading(self):
        consumed = []

        def rows():
            consumed.append(1)
            yield [1]

        streaming = KustoStreamingResultTable({"TableName": "T", "Columns": [{"ColumnName": "c", "ColumnType": "long"}], "Rows": rows()})
        reader = arrow_reader_from_result_table(streaming)
        assert reader.schema.field("c").type == pa.int64()
        assert consumed == []

    def test_reader_with_function_reads_ahead(self):
        streaming = KustoStreamingResultTable({"TableName": "T", "Columns": [{"ColumnName": "c", "ColumnType": "long"}], "Rows": iter([[1], [2], [3]])})
        reader = arrow_reader_from_result_table(
            streaming, batch_size=2, converters_by_column_name={"c": lambda name, values: pa.array(list(values), type=pa.int16())}
        )
        assert reader.schema.field("c").type == pa.int16()
        assert reader.read_all().column("c").to_pylist() == [1, 2, 3]

    def test_reader_with_function_on_empty_table(self):
        reader = arrow_reader_from_result_table(
            make_table("long", []), converters_by_column_name={"c": lambda name, values: pa.array(list(values), type=pa.int16())}
        )
        assert reader.schema.field("c").type == pa.int16()
        assert reader.read_all().num_rows == 0


class TestOptionalDependency:
    def test_helpers_do_not_need_pyarrow(self):
        code = "import sys; sys.modules['pyarrow'] = None; import azure.kusto.data, azure.kusto.data.helpers, azure.kusto.data.client"
        subprocess.run([sys.executable, "-c", code], check=True)

    def test_friendly_error(self, monkeypatch):
        import importlib

        monkeypatch.setitem(sys.modules, "pyarrow", None)
        monkeypatch.delitem(sys.modules, "azure.kusto.data.arrow_helpers")
        with pytest.raises(KustoArrowNotInstalledError, match=r"azure-kusto-data\[pyarrow\]"):
            importlib.import_module("azure.kusto.data.arrow_helpers")
