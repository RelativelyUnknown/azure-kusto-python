# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
"""
Conversion of asynchronous streaming Kusto results into `Apache Arrow <https://arrow.apache.org/>`_ structures.

This module requires the optional `pyarrow` and `aiohttp` dependencies: `pip install azure-kusto-data[pyarrow,aio]`.

The tables of a response of the asynchronous `KustoClient.execute_query` and `execute_mgmt` are ordinary `KustoResultTable`s, which are
converted with the functions of `azure.kusto.data.arrow_helpers`. The tables of a response of `KustoClient.execute_streaming_query`
are read from the network while they are iterated, and are converted here:

* `arrow_table_from_streaming_result_table` - reads a streaming table and returns it as a `pyarrow.Table`.
* `arrow_record_batches_from_streaming_result_table` - an asynchronous iterator of `pyarrow.RecordBatch`, which reads the table
  batch by batch, without materializing it in memory.
* `arrow_tables_from_streaming_response` - reads all (or only the primary) tables of a streaming response.

See `azure.kusto.data.arrow_helpers` for the type mapping and for the options, which are the same here.
The conversion of each batch is done by the event loop's thread.
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
    time_unit: TimeUnit = "ns",
    decimal_as: DecimalAs = "float64",
    decimal_precision: int = 38,
    decimal_scale: int = 18,
    guid_as: GuidAs = "string",
    errors: Errors = "coerce",
    converters_by_type: Optional[Dict[str, ArrowConverter]] = None,
    converters_by_column_name: Optional[Dict[str, ArrowConverter]] = None,
) -> "pa.Table":
    """Reads an asynchronous streaming Kusto result table entirely, and converts it into a pyarrow Table.
    The streaming table can't be read again.
    See `azure.kusto.data.arrow_helpers` for the options.

    :param table: Table received from the streaming response.
    :param batch_size: The number of rows converted at once.
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
    time_unit: TimeUnit = "ns",
    decimal_as: DecimalAs = "float64",
    decimal_precision: int = 38,
    decimal_scale: int = 18,
    guid_as: GuidAs = "string",
    errors: Errors = "coerce",
    converters_by_type: Optional[Dict[str, ArrowConverter]] = None,
    converters_by_column_name: Optional[Dict[str, ArrowConverter]] = None,
) -> AsyncIterator["pa.RecordBatch"]:
    """Converts an asynchronous streaming Kusto result table into pyarrow RecordBatches, lazily: a batch is read from the network
    only when it is requested, with `async for`. An empty table yields no batches.
    See `azure.kusto.data.arrow_helpers` for the options.

    :param table: Table received from the streaming response.
    :param batch_size: The (maximal) number of rows in each batch.
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
    time_unit: TimeUnit = "ns",
    decimal_as: DecimalAs = "float64",
    decimal_precision: int = 38,
    decimal_scale: int = 18,
    guid_as: GuidAs = "string",
    errors: Errors = "coerce",
    converters_by_type: Optional[Dict[str, ArrowConverter]] = None,
    converters_by_column_name: Optional[Dict[str, ArrowConverter]] = None,
) -> List["pa.Table"]:
    """Reads the tables of an asynchronous streaming Kusto response one after the other, and converts them into pyarrow Tables.
    See `azure.kusto.data.arrow_helpers` for the options.

    :param response: The response of the asynchronous `KustoClient.execute_streaming_query`.
    :param primary_only: When True (the default), only the primary result tables are converted. Otherwise, all of the tables are,
        including the tables with information about the query.
    :param batch_size: The number of rows converted at once.
    :return: A list of pyarrow Tables.
    """
    if not isinstance(response, KustoStreamingResponseDataSet):
        raise TypeError("Expected the asynchronous KustoStreamingResponseDataSet got {}".format(type(response).__name__))

    tables = []
    async for table in response.iter_primary_results() if primary_only else response:
        if table is None:  # The response marks its end with a None
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
