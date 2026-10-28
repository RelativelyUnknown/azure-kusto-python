# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
"""
Convert asynchronous streaming Kusto results to `Apache Arrow <https://arrow.apache.org/>`_ structures.

This module requires the optional `pyarrow` and `aiohttp` packages. Install them with `pip install azure-kusto-data[pyarrow,aio]`.

The tables in a response of the asynchronous `KustoClient.execute_query` and `execute_mgmt` are ordinary `KustoResultTable` objects.
Convert them with the functions of `azure.kusto.data.arrow_helpers`. The tables in a response of `KustoClient.execute_streaming_query`
are read from the network while you iterate them. Convert them with these functions:

* `arrow_table_from_streaming_result_table` - reads a streaming table and returns it as a `pyarrow.Table`.
* `arrow_record_batches_from_streaming_result_table` - returns an asynchronous iterator of `pyarrow.RecordBatch` that reads the table
  batch by batch, without holding it in memory.
* `arrow_tables_from_streaming_response` - reads all the tables of a streaming response, or only the primary ones.

See `azure.kusto.data.arrow_helpers` for the type mapping and the options, which are the same here.
The thread of the event loop converts each batch.
"""

from typing import Any, AsyncIterator, Dict, List, Optional, Union

from azure.kusto.data._models import BaseStreamingKustoResultTable, KustoResultTable
from azure.kusto.data.aio._models import KustoStreamingResultTable
from azure.kusto.data.aio.response import KustoStreamingResponseDataSet
from azure.kusto.data.arrow_helpers import (
    DEFAULT_BATCH_SIZE,
    ArrowConverter,
    DecimalAs,
    Errors,
    GuidAs,
    TimeUnit,
    _ArrowConverter,
    _check_batch_size,
    _make_converter,
)

import pyarrow as pa  # noqa: E402 - arrow_helpers has already made sure it's installed

__all__ = [
    "arrow_table_from_streaming_result_table",
    "arrow_record_batches_from_streaming_result_table",
    "arrow_tables_from_streaming_response",
]


def _check_table(table: Any) -> None:
    """Validates a table given to the asynchronous conversion functions."""
    if not table:
        raise ValueError("The table is missing or has no columns")
    if not isinstance(table, (KustoResultTable, KustoStreamingResultTable)):
        if isinstance(table, BaseStreamingKustoResultTable):
            raise TypeError(
                "Synchronous streaming tables are converted with azure.kusto.data.arrow_helpers, got {}".format(
                    type(table).__module__ + "." + type(table).__name__
                )
            )
        raise TypeError("Expected KustoResultTable or the asynchronous KustoStreamingResultTable got {}".format(type(table).__name__))


async def _iter_batches(
    converter: _ArrowConverter, table: Union[KustoResultTable, KustoStreamingResultTable], batch_size: int
) -> AsyncIterator["pa.RecordBatch"]:
    if isinstance(table, KustoResultTable):
        # Already in memory, e.g. a table which is not a primary result of a streaming response
        for batch in converter.iter_batches(table.raw_rows, batch_size):
            yield batch
        return

    chunk: List[Any] = []
    # The rows are read through a wrapper which keeps the table's state (row count, finished) up to date
    async for row in table._aiter_raw_rows():
        chunk.append(row)
        if len(chunk) == batch_size:
            yield converter.convert_rows(chunk)
            chunk = []
    if chunk:
        yield converter.convert_rows(chunk)


async def arrow_table_from_streaming_result_table(
    table: Union[KustoResultTable, KustoStreamingResultTable],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    time_unit: TimeUnit = "us",
    decimal_as: DecimalAs = "float64",
    decimal_precision: int = 38,
    decimal_scale: int = 18,
    guid_as: GuidAs = "string",
    errors: Errors = "coerce",
    converters_by_type: Optional[Dict[str, ArrowConverter]] = None,
    converters_by_column_name: Optional[Dict[str, ArrowConverter]] = None,
) -> "pa.Table":
    """Reads an asynchronous streaming Kusto result table entirely, and converts it to a pyarrow Table.
    The streaming table can't be read again.
    See `azure.kusto.data.arrow_helpers` for the options.

    :param table: Table received from the streaming response.
    :param batch_size: The number of rows converted at once. Defaults to `DEFAULT_BATCH_SIZE` (65,536).
    :raises TypeError: If `table` isn't a `KustoResultTable` or an asynchronous `KustoStreamingResultTable`, a converter is neither
        an Arrow type nor a function, or a converter function doesn't return an Arrow array.
    :raises ValueError: If `table` has no columns, `batch_size` is less than 1, an option has an invalid value, a column has a type that
        Kusto doesn't have and no converter, a row has more values than the table has columns, or a value can't be converted
        and `errors="raise"`.
    :raises azure.kusto.data.exceptions.KustoMultiApiError: If the service reports an error while the table is read.
    :return: pyarrow Table.
    """
    _check_batch_size(batch_size)
    converter = _make_converter(
        table, time_unit, decimal_as, decimal_precision, decimal_scale, guid_as, errors, converters_by_type, converters_by_column_name, _check_table
    )
    batches = [batch async for batch in _iter_batches(converter, table, batch_size)]
    return pa.Table.from_batches(batches, schema=converter.resolve_schema())


def arrow_record_batches_from_streaming_result_table(
    table: Union[KustoResultTable, KustoStreamingResultTable],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    time_unit: TimeUnit = "us",
    decimal_as: DecimalAs = "float64",
    decimal_precision: int = 38,
    decimal_scale: int = 18,
    guid_as: GuidAs = "string",
    errors: Errors = "coerce",
    converters_by_type: Optional[Dict[str, ArrowConverter]] = None,
    converters_by_column_name: Optional[Dict[str, ArrowConverter]] = None,
) -> AsyncIterator["pa.RecordBatch"]:
    """Converts an asynchronous streaming Kusto result table to pyarrow RecordBatches, lazily: a batch is read from the network
    only when you request it with `async for`. An empty table yields no batches.
    See `azure.kusto.data.arrow_helpers` for the options.

    :param table: Table received from the streaming response.
    :param batch_size: The maximal number of rows in each batch. Defaults to `DEFAULT_BATCH_SIZE` (65,536).
    :raises TypeError: If `table` isn't a `KustoResultTable` or an asynchronous `KustoStreamingResultTable`, or a converter is neither
        an Arrow type nor a function. Raised when you call the function. While you iterate, also if a converter function doesn't
        return an Arrow array.
    :raises ValueError: If `table` has no columns, `batch_size` is less than 1, an option has an invalid value, or a column has a type that
        Kusto doesn't have and no converter. Raised when you call the function. While you iterate, also if a row has more values than
        the table has columns, or a value can't be converted and `errors="raise"`.
    :raises azure.kusto.data.exceptions.KustoMultiApiError: While you iterate, if the service reports an error.
    :return: An asynchronous iterator of pyarrow RecordBatches.
    """
    _check_batch_size(batch_size)
    converter = _make_converter(
        table, time_unit, decimal_as, decimal_precision, decimal_scale, guid_as, errors, converters_by_type, converters_by_column_name, _check_table
    )
    return _iter_batches(converter, table, batch_size)


async def arrow_tables_from_streaming_response(
    response: KustoStreamingResponseDataSet,
    *,
    primary_only: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    time_unit: TimeUnit = "us",
    decimal_as: DecimalAs = "float64",
    decimal_precision: int = 38,
    decimal_scale: int = 18,
    guid_as: GuidAs = "string",
    errors: Errors = "coerce",
    converters_by_type: Optional[Dict[str, ArrowConverter]] = None,
    converters_by_column_name: Optional[Dict[str, ArrowConverter]] = None,
) -> List["pa.Table"]:
    """Reads the tables of an asynchronous streaming Kusto response one after the other, and converts them to pyarrow Tables.
    See `azure.kusto.data.arrow_helpers` for the options.

    :param response: The response of the asynchronous `KustoClient.execute_streaming_query`.
    :param primary_only: If True, converts only the primary result tables. Otherwise, converts all the tables, including the tables with
        information about the query. Defaults to True.
    :param batch_size: The number of rows converted at once. Defaults to `DEFAULT_BATCH_SIZE` (65,536).
    :raises TypeError: If `response` isn't an asynchronous `KustoStreamingResponseDataSet`, or a converter is neither an Arrow type nor
        a function.
    :raises ValueError: If a table has no columns, `batch_size` is less than 1, an option has an invalid value, a column has a type that
        Kusto doesn't have and no converter, a row has more values than the table has columns, or a value can't be converted
        and `errors="raise"`.
    :raises azure.kusto.data.exceptions.KustoMultiApiError: If the service reports an error while the response is read.
    :return: A list of pyarrow Tables.
    """
    if not isinstance(response, KustoStreamingResponseDataSet):
        raise TypeError("Expected the asynchronous KustoStreamingResponseDataSet got {}".format(type(response).__name__))

    tables = []
    async for table in response.iter_primary_results() if primary_only else response:
        if not isinstance(table, (KustoResultTable, KustoStreamingResultTable)):  # The response marks its end with a None
            continue
        tables.append(
            await arrow_table_from_streaming_result_table(
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
        )
    return tables
