# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
"""
Conversion of Kusto results into `Apache Arrow <https://arrow.apache.org/>`_ structures.

This module requires the optional `pyarrow` dependency: `pip install azure-kusto-data[pyarrow]`.

The supported outputs are:

* `arrow_table_from_result_table` - a single result table (full or streaming) as a `pyarrow.Table`.
* `arrow_tables_from_response` - all (or only the primary) tables of a response as a list of `pyarrow.Table`.
* `arrow_record_batches_from_result_table` / `arrow_reader_from_result_table` - a (streaming) result table as record batches,
  without materializing it in memory.
* `arrow_schema_from_result_table` - the Arrow schema a table would be converted to.

For asynchronous streaming results, see `azure.kusto.data.aio.arrow_helpers`.

Type mapping (the default, can be changed with the options below or with converters):

=====================  ==============================  =================================================
Kusto type             Arrow type                      Notes
=====================  ==============================  =================================================
bool                   bool                            nulls are preserved
int                    int32
long                   int64
real                   float64                         `NaN`, `Infinity` and `-Infinity` are supported
decimal                float64                         see `decimal_as`
string                 string                          empty strings are kept as empty strings
guid                   string                          see `guid_as`
datetime               timestamp[ns, UTC]              see `time_unit`
timespan               duration[ns]                    see `time_unit`
dynamic                string                          compact JSON text
=====================  ==============================  =================================================

Every field of the resulting schema carries the original Kusto type in its metadata (`kusto.type`), and the schema
carries the table's name, id and kind (`kusto.table_name`, `kusto.table_id` and `kusto.table_kind`).

Options shared by the conversion functions:

:param time_unit: The resolution of the Arrow `timestamp` and `duration` types, `"ns"` (the default) or `"us"`.
    Kusto keeps time in ticks of 100 nanoseconds, in the years 0001-9999. Arrow's nanosecond resolution preserves every tick
    but can only represent the years 1677-2262 (for timespans: about 292 years); values outside of this range, like
    `datetime(0001-01-01)` or `datetime(9999-12-31)`, are converted to nulls (or raise, see `errors`).
    The microsecond resolution covers the entire Kusto range, but truncates the last digit of the fraction of a second.
:param decimal_as: How to convert `decimal` columns: `"float64"` (the default, might lose precision), `"decimal128"` (exact,
    using `decimal_precision` and `decimal_scale`, since Kusto does not report them) or `"string"`.
:param decimal_precision: The precision of the Arrow `decimal128` type, when `decimal_as="decimal128"`.
:param decimal_scale: The scale of the Arrow `decimal128` type, when `decimal_as="decimal128"`.
:param guid_as: How to convert `guid` columns: `"string"` (the default) or `"uuid"` (the Arrow canonical `uuid` extension type,
    which is the type to use when ingesting the data back into a `guid` column).
:param errors: What to do with a value that can't be converted: `"coerce"` (the default) converts it to null,
    `"raise"` raises a `ValueError`.
:param converters_by_type: If given, converts the columns of the specified Kusto types differently than the default. The dictionary
    maps from a Kusto data type (https://learn.microsoft.com/azure/data-explorer/kusto/query/scalar-data-types/) to either an Arrow
    type (the values are converted with `pyarrow.array(values, type=...)`) or to a function that receives the column name and the
    sequence of raw values of the column, and returns a `pyarrow.Array` of the same length.
:param converters_by_column_name: Same as `converters_by_type`, but by column name. It takes precedence over `converters_by_type`.
"""

import json
import re
import uuid
from datetime import timezone
from decimal import Context, Decimal
from itertools import chain, islice
from typing import Any, Callable, Dict, Iterable, Iterator, List, Literal, Optional, Sequence, Tuple, Union

from dateutil.parser import isoparse

from ._models import BaseStreamingKustoResultTable, KustoResultColumn, KustoResultTable, KustoStreamingResultTable
from .exceptions import KustoArrowNotInstalledError
from .response import BaseKustoResponseDataSet, KustoResponseDataSet, KustoStreamingResponseDataSet

try:
    import pyarrow as pa
    import pyarrow.compute as _pc
except ImportError as e:
    raise KustoArrowNotInstalledError() from e

# The compute functions are generated when pyarrow is imported, so type checkers don't know them
pc: Any = _pc

__all__ = [
    "DEFAULT_BATCH_SIZE",
    "KUSTO_TYPE_METADATA_KEY",
    "KUSTO_TABLE_NAME_METADATA_KEY",
    "KUSTO_TABLE_ID_METADATA_KEY",
    "KUSTO_TABLE_KIND_METADATA_KEY",
    "ArrowConverter",
    "arrow_schema_from_result_table",
    "arrow_table_from_result_table",
    "arrow_tables_from_response",
    "arrow_record_batches_from_result_table",
    "arrow_reader_from_result_table",
]

DEFAULT_BATCH_SIZE = 65_536
"""The default number of rows converted at once."""

KUSTO_TYPE_METADATA_KEY = "kusto.type"
"""The key of the field metadata which holds the original Kusto type of the column."""
KUSTO_TABLE_NAME_METADATA_KEY = "kusto.table_name"
KUSTO_TABLE_ID_METADATA_KEY = "kusto.table_id"
KUSTO_TABLE_KIND_METADATA_KEY = "kusto.table_kind"

TimeUnit = Literal["ns", "us"]
DecimalAs = Literal["float64", "decimal128", "string"]
GuidAs = Literal["string", "uuid"]
Errors = Literal["coerce", "raise"]

ArrowConverter = Union["pa.DataType", Callable[[str, Sequence[Any]], "pa.Array"]]
"""Alias for the converters of `converters_by_type` and `converters_by_column_name`."""

# Kusto type names (lower cased) and their canonical Kusto type. The first group is what the service reports, the second is
# the .NET type name, which older responses of the management API report in "DataType".
_CANONICAL_TYPES = {
    "bool": "bool",
    "boolean": "bool",
    "int": "int",
    "int32": "int",
    "long": "long",
    "int64": "long",
    "real": "real",
    "double": "real",
    "decimal": "decimal",
    "string": "string",
    "guid": "guid",
    "uuid": "guid",
    "uniqueid": "guid",
    "datetime": "datetime",
    "date": "datetime",
    "timespan": "timespan",
    "time": "timespan",
    "dynamic": "dynamic",
    "sbyte": "bool",
    "sqldecimal": "decimal",
    "object": "dynamic",
}

_TICKS_PER_SECOND = 10_000_000
_MAX_NS_TICKS = (2**63 - 1) // 100  # The largest number of ticks which fits in 64 bits when converted to nanoseconds
_EPOCH_ORDINAL = 719163  # datetime.date(1970, 1, 1).toordinal()

# Kusto datetimes are always UTC, and are formatted as 2014-01-01T01:01:01.0000001Z (the fraction is optional)
_DATETIME_PATTERN = r"^(?P<head>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(?P<frac>\d{1,9}))?Z$"
_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
# Kusto timespans are formatted as [-][d.]hh:mm:ss[.fffffff]
_TIMESPAN_PATTERN = re.compile(r"^(?P<neg>-)?(?:(?P<days>\d+)\.)?(?P<hours>\d{1,2}):(?P<minutes>\d{2}):(?P<seconds>\d{2})(?:\.(?P<fraction>\d{1,7}))?$")

_CONVERSION_ERRORS = (ValueError, TypeError, ArithmeticError)
_DECIMAL_CONTEXT = Context(prec=100)


def _canonical_type(column_type: str) -> Optional[str]:
    return _CANONICAL_TYPES.get(column_type.lower())


def _to_bool(value: Any) -> bool:
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("true", "1"):
            return True
        if text in ("false", "0"):
            return False
        raise ValueError("not a boolean")
    if isinstance(value, (bool, int, float)):
        return bool(value)
    raise TypeError("not a boolean")


def _to_int(value: Any, bits: int) -> int:
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError("not an integer")
        number = int(value)
    elif isinstance(value, (bool, int)):
        number = int(value)
    elif isinstance(value, str):
        number = int(value.strip())
    else:
        raise TypeError("not an integer")
    limit = 1 << (bits - 1)
    if not -limit <= number < limit:
        raise OverflowError("out of the range of a {}-bit integer".format(bits))
    return number


def _to_decimal(value: Any, precision: int, scale: int) -> Decimal:
    if isinstance(value, Decimal):
        number = value
    elif isinstance(value, bool):
        raise TypeError("not a decimal")
    elif isinstance(value, int):
        number = Decimal(value)
    elif isinstance(value, float):
        number = Decimal(repr(value))
    elif isinstance(value, str):
        number = Decimal(value.strip())
    else:
        raise TypeError("not a decimal")
    if not number.is_finite():
        raise ValueError("decimal128 can't represent NaN or infinity")
    number = number.quantize(Decimal(1).scaleb(-scale), context=_DECIMAL_CONTEXT)
    if len(number.as_tuple().digits) > precision:
        raise OverflowError("out of the range of decimal128({}, {})".format(precision, scale))
    return number


def _to_uuid_bytes(value: Any) -> Optional[bytes]:
    if isinstance(value, uuid.UUID):
        return value.bytes
    if isinstance(value, str):
        text = value.strip()
        # Kusto represents a null guid with an empty string in some responses
        return uuid.UUID(text).bytes if text else None
    raise TypeError("not a guid")


def _to_json(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _datetime_to_ticks(value: str) -> int:
    """Fallback for datetime strings which are not formatted the way Kusto formats them."""
    parsed = isoparse(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    seconds = (parsed.toordinal() - _EPOCH_ORDINAL) * 86400 + parsed.hour * 3600 + parsed.minute * 60 + parsed.second
    return seconds * _TICKS_PER_SECOND + parsed.microsecond * 10


def _timespan_to_ticks(value: Any) -> int:
    """Converts a raw Kusto timespan to a number of 100 nanoseconds ticks. A number is already a number of ticks."""
    if isinstance(value, bool):
        raise TypeError("not a timespan")
    if isinstance(value, (int, float)):
        return int(value)
    if not isinstance(value, str):
        raise TypeError("not a timespan")
    match = _TIMESPAN_PATTERN.match(value.strip())
    if not match:
        raise ValueError("not a timespan")
    seconds = ((int(match.group("days") or 0) * 24 + int(match.group("hours"))) * 60 + int(match.group("minutes"))) * 60 + int(match.group("seconds"))
    ticks = seconds * _TICKS_PER_SECOND + int((match.group("fraction") or "").ljust(7, "0"))
    return -ticks if match.group("neg") else ticks


def _timespan_to_nanoseconds(value: Any) -> int:
    ticks = _timespan_to_ticks(value)
    if not -_MAX_NS_TICKS <= ticks <= _MAX_NS_TICKS:
        raise OverflowError("out of the range of a nanosecond resolution duration")
    return ticks * 100


def _timespan_to_microseconds(value: Any) -> int:
    ticks = _timespan_to_ticks(value)
    microseconds = abs(ticks) // 10  # Truncated towards zero
    return -microseconds if ticks < 0 else microseconds


class _ArrowConverter:
    """
    Converts raw Kusto rows (lists of the JSON values of the response) to Arrow record batches.
    One instance per table, since the (optionally lazily resolved) schema is bound to it.
    """

    def __init__(
        self,
        columns: Sequence[KustoResultColumn],
        metadata: Dict[str, str],
        time_unit: TimeUnit,
        decimal_as: DecimalAs,
        decimal_precision: int,
        decimal_scale: int,
        guid_as: GuidAs,
        errors: Errors,
        converters_by_type: Optional[Dict[str, ArrowConverter]],
        converters_by_column_name: Optional[Dict[str, ArrowConverter]],
    ):
        if time_unit not in ("ns", "us"):
            raise ValueError("time_unit must be 'ns' or 'us', got {!r}".format(time_unit))
        if decimal_as not in ("float64", "decimal128", "string"):
            raise ValueError("decimal_as must be 'float64', 'decimal128' or 'string', got {!r}".format(decimal_as))
        if not 1 <= decimal_precision <= 38 or not 0 <= decimal_scale <= decimal_precision:
            raise ValueError("decimal_precision must be between 1 and 38 and decimal_scale between 0 and decimal_precision")
        if guid_as not in ("string", "uuid"):
            raise ValueError("guid_as must be 'string' or 'uuid', got {!r}".format(guid_as))
        if errors not in ("coerce", "raise"):
            raise ValueError("errors must be 'coerce' or 'raise', got {!r}".format(errors))

        self._time_unit = time_unit
        self._decimal_as = decimal_as
        self._decimal_precision = decimal_precision
        self._decimal_scale = decimal_scale
        self._guid_as = guid_as
        self._raise_errors = errors == "raise"
        self._metadata = metadata

        self._columns = list(columns)
        # For every column: (name, Kusto type, Arrow type or None when it is only known after the first conversion, builder)
        self._plans: List[Tuple[str, str, Optional["pa.DataType"], Callable[[str, Sequence[Any]], "pa.Array"]]] = []
        for column in self._columns:
            self._plans.append(self._plan(column, converters_by_type, converters_by_column_name))

        self._schema: Optional["pa.Schema"] = None
        if all(plan[2] is not None for plan in self._plans):
            self._schema = self._make_schema([plan[2] for plan in self._plans])

    @property
    def schema(self) -> Optional["pa.Schema"]:
        """The schema, or None if it is not known yet, because a callable converter was given and nothing was converted yet."""
        return self._schema

    # ---- planning

    def _plan(
        self,
        column: KustoResultColumn,
        converters_by_type: Optional[Dict[str, ArrowConverter]],
        converters_by_column_name: Optional[Dict[str, ArrowConverter]],
    ) -> Tuple[str, str, Optional["pa.DataType"], Callable[[str, Sequence[Any]], "pa.Array"]]:
        name, raw_type = column.column_name, column.column_type
        canonical = _canonical_type(raw_type)
        kusto_type = canonical or raw_type

        converter: Optional[ArrowConverter] = None
        if converters_by_column_name and name in converters_by_column_name:
            converter = converters_by_column_name[name]
        elif converters_by_type and raw_type in converters_by_type:
            converter = converters_by_type[raw_type]
        elif converters_by_type and canonical is not None and canonical in converters_by_type:
            converter = converters_by_type[canonical]

        if converter is not None:
            if isinstance(converter, pa.DataType):
                return name, kusto_type, converter, self._explicit_builder(converter)
            if callable(converter):
                return name, kusto_type, None, self._callable_builder(converter)
            raise TypeError("The converter of column '{}' must be an Arrow type or a function, got {}".format(name, type(converter).__name__))

        if canonical is None:
            raise ValueError("Unexpected type '{}' of column '{}'".format(raw_type, name))
        arrow_type, builder = self._default(canonical)
        return name, kusto_type, arrow_type, builder

    def _default(self, canonical: str) -> Tuple["pa.DataType", Callable[[str, Sequence[Any]], "pa.Array"]]:
        if canonical == "bool":
            return pa.bool_(), self._build_bool
        if canonical == "int":
            return pa.int32(), lambda name, values: self._build_int(name, values, pa.int32())
        if canonical == "long":
            return pa.int64(), lambda name, values: self._build_int(name, values, pa.int64())
        if canonical == "real":
            return pa.float64(), self._build_float
        if canonical == "decimal":
            if self._decimal_as == "decimal128":
                return pa.decimal128(self._decimal_precision, self._decimal_scale), self._build_decimal128
            if self._decimal_as == "string":
                return pa.string(), self._build_decimal_string
            return pa.float64(), self._build_float
        if canonical == "string":
            return pa.string(), self._build_string
        if canonical == "guid":
            if self._guid_as == "uuid":
                return pa.uuid(), self._build_uuid
            return pa.string(), self._build_string
        if canonical == "datetime":
            return pa.timestamp(self._time_unit, "UTC"), self._build_datetime
        if canonical == "timespan":
            return pa.duration(self._time_unit), self._build_timespan
        return pa.string(), self._build_dynamic

    def _make_schema(self, types: Sequence["pa.DataType"]) -> "pa.Schema":
        fields = [
            pa.field(name, arrow_type, metadata={KUSTO_TYPE_METADATA_KEY: kusto_type}) for (name, kusto_type, _, _), arrow_type in zip(self._plans, types)
        ]
        return pa.schema(fields, metadata=self._metadata)

    # ---- conversion of rows

    def convert_rows(self, rows: Sequence[Sequence[Any]]) -> "pa.RecordBatch":
        """Converts a chunk of raw rows to a record batch."""
        columns_count = len(self._plans)
        if rows:
            if set(map(len, rows)) != {columns_count}:
                rows = [self._fit_row(row, columns_count) for row in rows]
            values_by_column: List[Sequence[Any]] = list(zip(*rows))
        else:
            values_by_column = [()] * columns_count

        arrays = []
        for (name, _, _, builder), values in zip(self._plans, values_by_column):
            array = builder(name, values)
            if len(array) != len(values):
                raise ValueError("The converter of column '{}' returned {} values for {} rows".format(name, len(array), len(values)))
            arrays.append(array)

        if self._schema is None:
            self._schema = self._make_schema([array.type for array in arrays])
        else:
            for (name, _, _, _), array, field in zip(self._plans, arrays, self._schema):
                if not array.type.equals(field.type):
                    raise ValueError("The converter of column '{}' returned {} values, but previously returned {}".format(name, array.type, field.type))
        return pa.RecordBatch.from_arrays(arrays, schema=self._schema)

    @staticmethod
    def _fit_row(row: Sequence[Any], columns_count: int) -> Sequence[Any]:
        if len(row) > columns_count:
            raise ValueError("A row has {} values, but the table has {} columns".format(len(row), columns_count))
        # Pad the missing values with nulls, rather than silently shifting the data
        return list(row) + [None] * (columns_count - len(row))

    def iter_batches(self, rows: Iterable[Sequence[Any]], batch_size: int) -> Iterator["pa.RecordBatch"]:
        """Converts rows to record batches of (at most) `batch_size` rows."""
        if batch_size < 1:
            raise ValueError("batch_size must be positive, got {}".format(batch_size))
        if isinstance(rows, list):
            for start in range(0, len(rows), batch_size):
                yield self.convert_rows(rows[start : start + batch_size])
            return
        iterator = iter(rows)
        while True:
            chunk = list(islice(iterator, batch_size))
            if not chunk:
                return
            yield self.convert_rows(chunk)

    def resolve_schema(self) -> "pa.Schema":
        """Returns the schema, converting an empty chunk if needed to learn the types returned by callable converters."""
        if self._schema is None:
            self.convert_rows([])
        assert self._schema is not None
        return self._schema

    # ---- builders for explicit converters

    @staticmethod
    def _explicit_builder(arrow_type: "pa.DataType") -> Callable[[str, Sequence[Any]], "pa.Array"]:
        return lambda name, values: pa.array(values, type=arrow_type)

    @staticmethod
    def _callable_builder(converter: Callable[[str, Sequence[Any]], "pa.Array"]) -> Callable[[str, Sequence[Any]], "pa.Array"]:
        def build(name: str, values: Sequence[Any]) -> "pa.Array":
            result = converter(name, values)
            if isinstance(result, pa.ChunkedArray):
                result = result.combine_chunks()
            if not isinstance(result, pa.Array):
                raise TypeError("The converter of column '{}' must return an Arrow array, got {}".format(name, type(result).__name__))
            return result

        return build

    # ---- builders for the default conversions

    def _map(self, name: str, values: Sequence[Any], convert: Callable[[Any], Any], target: str) -> List[Any]:
        """Converts the values one by one, applying the error policy. Nulls stay nulls."""
        result = []
        for value in values:
            if value is None:
                result.append(None)
                continue
            try:
                result.append(convert(value))
            except _CONVERSION_ERRORS as e:
                if self._raise_errors:
                    raise ValueError("Can't convert the value {!r} of column '{}' to {}: {}".format(value, name, target, e)) from e
                result.append(None)
        return result

    def _build_bool(self, name: str, values: Sequence[Any]) -> "pa.Array":
        try:
            return pa.array(values, type=pa.bool_())
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            return pa.array(self._map(name, values, _to_bool, "bool"), type=pa.bool_())

    def _build_int(self, name: str, values: Sequence[Any], arrow_type: "pa.DataType") -> "pa.Array":
        try:
            return pa.array(values, type=arrow_type)
        except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
            bits = arrow_type.bit_width
            return pa.array(self._map(name, values, lambda value: _to_int(value, bits), str(arrow_type)), type=arrow_type)

    def _build_float(self, name: str, values: Sequence[Any]) -> "pa.Array":
        try:
            return pa.array(values, type=pa.float64())
        except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
            # Kusto sends the special values as the strings "NaN", "Infinity" and "-Infinity", which float() understands
            return pa.array(self._map(name, values, float, "float64"), type=pa.float64())

    def _build_decimal128(self, name: str, values: Sequence[Any]) -> "pa.Array":
        precision, scale = self._decimal_precision, self._decimal_scale
        converted = self._map(name, values, lambda value: _to_decimal(value, precision, scale), "decimal128({}, {})".format(precision, scale))
        return pa.array(converted, type=pa.decimal128(precision, scale))

    def _build_decimal_string(self, name: str, values: Sequence[Any]) -> "pa.Array":
        return pa.array(self._map(name, values, lambda value: value if isinstance(value, str) else repr(value), "string"), type=pa.string())

    def _build_string(self, name: str, values: Sequence[Any]) -> "pa.Array":
        try:
            return pa.array(values, type=pa.string())
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            return pa.array(self._map(name, values, lambda value: value if isinstance(value, str) else str(value), "string"), type=pa.string())

    def _build_uuid(self, name: str, values: Sequence[Any]) -> "pa.Array":
        storage = pa.array(self._map(name, values, _to_uuid_bytes, "uuid"), type=pa.binary(16))
        return pa.ExtensionArray.from_storage(pa.uuid(), storage)

    def _build_dynamic(self, name: str, values: Sequence[Any]) -> "pa.Array":
        return pa.array(self._map(name, values, _to_json, "JSON"), type=pa.string())

    def _build_timespan(self, name: str, values: Sequence[Any]) -> "pa.Array":
        if self._time_unit == "ns":
            ticks = self._map(name, values, _timespan_to_nanoseconds, "duration[ns]")
        else:
            ticks = self._map(name, values, _timespan_to_microseconds, "duration[us]")
        return pa.array(ticks, type=pa.int64()).cast(pa.duration(self._time_unit))

    def _build_datetime(self, name: str, values: Sequence[Any]) -> "pa.Array":
        # Everything is done with Arrow's vectorized kernels (the strings are validated by the regex, and parsed into seconds
        # and ticks), rather than with Arrow's string to timestamp cast: the cast rejects fractions of more than 6 digits when
        # the resolution is microseconds, and silently overflows in some versions of Arrow when the resolution is nanoseconds.
        try:
            strings = pa.array(values, type=pa.string())
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            strings = pa.array([None if value is None else str(value) for value in values], type=pa.string())

        match = pc.extract_regex(strings, _DATETIME_PATTERN)
        seconds = pc.strptime(pc.struct_field(match, "head"), _DATETIME_FORMAT, unit="s", error_is_null=True).cast(pa.int64())
        fraction = pc.fill_null(pc.struct_field(match, "frac"), "")
        ticks = pc.utf8_slice_codeunits(pc.utf8_rpad(fraction, 7, "0"), 0, 7).cast(pa.int64())
        total_ticks = pc.add(pc.multiply(seconds, _TICKS_PER_SECOND), ticks)

        unparsed = pc.and_(pc.is_null(seconds), pc.is_valid(strings))
        if pc.any(unparsed).as_py():
            total_ticks = self._parse_datetimes_slowly(name, strings, unparsed, total_ticks)

        if self._time_unit == "ns":
            in_range = pc.and_(pc.greater_equal(total_ticks, -_MAX_NS_TICKS), pc.less_equal(total_ticks, _MAX_NS_TICKS))
            if self._raise_errors:
                out_of_range = pc.fill_null(pc.invert(in_range), False)
                if pc.any(out_of_range).as_py():
                    value = pc.filter(strings, out_of_range)[0].as_py()
                    raise ValueError(
                        "The datetime {!r} of column '{}' can't be represented with a nanosecond resolution, use time_unit='us'".format(value, name)
                    )
            result = pc.multiply(pc.if_else(in_range, total_ticks, pa.scalar(None, pa.int64())), 100)
        else:
            # Floor division, so that the part before 1970 is rounded down as well
            quotient = pc.divide(total_ticks, 10)
            remainder = pc.subtract(total_ticks, pc.multiply(quotient, 10))
            result = pc.if_else(pc.less(remainder, 0), pc.subtract(quotient, 1), quotient)
        return result.cast(pa.timestamp(self._time_unit, "UTC"))

    def _parse_datetimes_slowly(self, name: str, strings: "pa.Array", unparsed: "pa.Array", total_ticks: "pa.Array") -> "pa.Array":
        """Parses the datetimes which are not formatted the way Kusto does, one by one."""
        ticks = total_ticks.to_pylist()
        for index, is_unparsed in enumerate(unparsed.to_pylist()):
            if not is_unparsed:
                continue
            value = strings[index].as_py()
            try:
                ticks[index] = _datetime_to_ticks(value)
            except _CONVERSION_ERRORS as e:
                if self._raise_errors:
                    raise ValueError("Can't convert the value {!r} of column '{}' to a datetime: {}".format(value, name, e)) from e
                ticks[index] = None
        return pa.array(ticks, type=pa.int64())


def _check_table(table: Any) -> None:
    """Validates a table given to the (synchronous) conversion functions."""
    if not table:
        raise ValueError("The table is missing or has no columns")
    if not isinstance(table, (KustoResultTable, KustoStreamingResultTable)):
        if isinstance(table, BaseStreamingKustoResultTable):
            raise TypeError(
                "Asynchronous streaming tables are converted with azure.kusto.data.aio.arrow_helpers, got {}".format(
                    type(table).__module__ + "." + type(table).__name__
                )
            )
        raise TypeError("Expected KustoResultTable or KustoStreamingResultTable got {}".format(type(table).__name__))


def _table_metadata(table: Any) -> Dict[str, str]:
    metadata = {}
    if table.table_name is not None:
        metadata[KUSTO_TABLE_NAME_METADATA_KEY] = str(table.table_name)
    if table.table_id is not None:
        metadata[KUSTO_TABLE_ID_METADATA_KEY] = str(table.table_id)
    if table.table_kind is not None:
        metadata[KUSTO_TABLE_KIND_METADATA_KEY] = table.table_kind.value
    return metadata


def _make_converter(
    table: Any,
    time_unit: TimeUnit,
    decimal_as: DecimalAs,
    decimal_precision: int,
    decimal_scale: int,
    guid_as: GuidAs,
    errors: Errors,
    converters_by_type: Optional[Dict[str, ArrowConverter]],
    converters_by_column_name: Optional[Dict[str, ArrowConverter]],
    check_table: Callable[[Any], None] = _check_table,
) -> _ArrowConverter:
    check_table(table)
    return _ArrowConverter(
        table.columns,
        _table_metadata(table),
        time_unit,
        decimal_as,
        decimal_precision,
        decimal_scale,
        guid_as,
        errors,
        converters_by_type,
        converters_by_column_name,
    )


def _check_batch_size(batch_size: int) -> None:
    if batch_size < 1:
        raise ValueError("batch_size must be positive, got {}".format(batch_size))


def _raw_rows(table: Union[KustoResultTable, KustoStreamingResultTable]) -> Iterable[List[Any]]:
    # Streaming tables are read through a wrapper which keeps their state (row count, finished) up to date
    if isinstance(table, KustoStreamingResultTable):
        return table._iter_raw_rows()
    return table.raw_rows


def arrow_schema_from_result_table(
    table: Union[KustoResultTable, KustoStreamingResultTable],
    *,
    time_unit: TimeUnit = "ns",
    decimal_as: DecimalAs = "float64",
    decimal_precision: int = 38,
    decimal_scale: int = 18,
    guid_as: GuidAs = "string",
    errors: Errors = "coerce",
    converters_by_type: Optional[Dict[str, ArrowConverter]] = None,
    converters_by_column_name: Optional[Dict[str, ArrowConverter]] = None,
) -> "pa.Schema":
    """Gets the Arrow schema a Kusto result table is converted to. No row is read from the table.
    See the module documentation for the options.

    :param table: Table received from the response.
    :raises ValueError: If a function is given as a converter, since the type it returns is only known after calling it.
    :return: pyarrow Schema.
    """
    converter = _make_converter(table, time_unit, decimal_as, decimal_precision, decimal_scale, guid_as, errors, converters_by_type, converters_by_column_name)
    if converter.schema is None:
        raise ValueError("The schema isn't known ahead of time when a function is given as a converter")
    return converter.schema


def arrow_table_from_result_table(
    table: Union[KustoResultTable, KustoStreamingResultTable],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    time_unit: TimeUnit = "ns",
    decimal_as: DecimalAs = "float64",
    decimal_precision: int = 38,
    decimal_scale: int = 18,
    guid_as: GuidAs = "string",
    errors: Errors = "coerce",
    converters_by_type: Optional[Dict[str, ArrowConverter]] = None,
    converters_by_column_name: Optional[Dict[str, ArrowConverter]] = None,
) -> "pa.Table":
    """Converts a Kusto result table into a pyarrow Table. The Arrow counterpart of `azure.kusto.data.helpers.dataframe_from_result_table`.
    See the module documentation for the options.

    A streaming table is read entirely, so it can't be read again. To keep the memory in check, use
    `arrow_reader_from_result_table`.

    :param table: Table received from the response.
    :param batch_size: The number of rows converted at once. The columns of the result consist of chunks of this size, call
        `combine_chunks` on the result if you need contiguous columns.
    :return: pyarrow Table.
    """
    _check_batch_size(batch_size)
    converter = _make_converter(table, time_unit, decimal_as, decimal_precision, decimal_scale, guid_as, errors, converters_by_type, converters_by_column_name)
    batches = list(converter.iter_batches(_raw_rows(table), batch_size))
    return pa.Table.from_batches(batches, schema=converter.resolve_schema())


def arrow_tables_from_response(
    response: BaseKustoResponseDataSet,
    *,
    primary_only: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    time_unit: TimeUnit = "ns",
    decimal_as: DecimalAs = "float64",
    decimal_precision: int = 38,
    decimal_scale: int = 18,
    guid_as: GuidAs = "string",
    errors: Errors = "coerce",
    converters_by_type: Optional[Dict[str, ArrowConverter]] = None,
    converters_by_column_name: Optional[Dict[str, ArrowConverter]] = None,
) -> List["pa.Table"]:
    """Converts the tables of a Kusto response into pyarrow Tables.
    See the module documentation for the options.

    The tables of a streaming response are read one after the other, so the entire response is read.

    :param response: The response of `KustoClient.execute`, `execute_query`, `execute_mgmt` or `execute_streaming_query`.
    :param primary_only: When True (the default), only the primary result tables are converted. Otherwise, all of the tables are,
        including the tables with information about the query.
    :param batch_size: The number of rows converted at once.
    :return: A list of pyarrow Tables.
    """
    if isinstance(response, KustoResponseDataSet):
        tables: Iterable[Any] = response.primary_results if primary_only else response.tables
    elif isinstance(response, KustoStreamingResponseDataSet):
        tables = response.iter_primary_results() if primary_only else response
    else:
        raise TypeError("Expected KustoResponseDataSet or KustoStreamingResponseDataSet got {}".format(type(response).__name__))

    return [
        arrow_table_from_result_table(
            table,
            batch_size=batch_size,
            time_unit=time_unit,
            decimal_as=decimal_as,
            decimal_precision=decimal_precision,
            decimal_scale=decimal_scale,
            guid_as=guid_as,
            errors=errors,
            converters_by_type=converters_by_type,
            converters_by_column_name=converters_by_column_name,
        )
        for table in tables
    ]


def arrow_record_batches_from_result_table(
    table: Union[KustoResultTable, KustoStreamingResultTable],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    time_unit: TimeUnit = "ns",
    decimal_as: DecimalAs = "float64",
    decimal_precision: int = 38,
    decimal_scale: int = 18,
    guid_as: GuidAs = "string",
    errors: Errors = "coerce",
    converters_by_type: Optional[Dict[str, ArrowConverter]] = None,
    converters_by_column_name: Optional[Dict[str, ArrowConverter]] = None,
) -> Iterator["pa.RecordBatch"]:
    """Converts a Kusto result table into pyarrow RecordBatches, lazily: a batch is read from the table (and from the network,
    in case of a streaming table) only when it is requested. An empty table yields no batches.
    See the module documentation for the options.

    :param table: Table received from the response.
    :param batch_size: The (maximal) number of rows in each batch.
    :return: An iterator of pyarrow RecordBatches.
    """
    _check_batch_size(batch_size)
    converter = _make_converter(table, time_unit, decimal_as, decimal_precision, decimal_scale, guid_as, errors, converters_by_type, converters_by_column_name)
    return converter.iter_batches(_raw_rows(table), batch_size)


def arrow_reader_from_result_table(
    table: Union[KustoResultTable, KustoStreamingResultTable],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    time_unit: TimeUnit = "ns",
    decimal_as: DecimalAs = "float64",
    decimal_precision: int = 38,
    decimal_scale: int = 18,
    guid_as: GuidAs = "string",
    errors: Errors = "coerce",
    converters_by_type: Optional[Dict[str, ArrowConverter]] = None,
    converters_by_column_name: Optional[Dict[str, ArrowConverter]] = None,
) -> "pa.RecordBatchReader":
    """Converts a Kusto result table into a pyarrow RecordBatchReader, which reads the table lazily, batch by batch.
    The reader can be consumed by anything which understands Arrow streams, like `pyarrow.parquet.ParquetWriter`.
    See the module documentation for the options.

    The schema of the reader is available before reading any row, unless a function was given as a converter, in which case the
    first batch is read ahead to learn it.

    :param table: Table received from the response.
    :param batch_size: The (maximal) number of rows in each batch.
    :return: pyarrow RecordBatchReader.
    """
    _check_batch_size(batch_size)
    converter = _make_converter(table, time_unit, decimal_as, decimal_precision, decimal_scale, guid_as, errors, converters_by_type, converters_by_column_name)
    batches = converter.iter_batches(_raw_rows(table), batch_size)
    if converter.schema is None:
        first = next(batches, None)
        schema = converter.resolve_schema()
        if first is not None:
            batches = chain([first], batches)
    else:
        schema = converter.schema
    return pa.RecordBatchReader.from_batches(schema, batches)
