# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
"""Writes Arrow data to a Parquet file, in a way Kusto can ingest. The pyarrow dependency is imported only when it is used."""

import contextlib
import os
import tempfile
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from azure.kusto.data.exceptions import KustoArrowNotInstalledError

# The first version of pyarrow which marks the columns of the uuid type with Parquet's UUID type. Kusto converts only this
# type to a guid column, an unmarked fixed size binary column can't be ingested to it.
_MIN_PYARROW_VERSION_FOR_UUID = 21


def _import_pyarrow() -> Tuple[Any, Any]:
    try:
        import pyarrow
        import pyarrow.parquet
    except ImportError as e:
        raise KustoArrowNotInstalledError("azure-kusto-ingest") from e
    return pyarrow, pyarrow.parquet


def _check_schema(pa: Any, schema: Any) -> None:
    """Raises if a column can't be ingested by Kusto from a Parquet file, rather than letting the ingestion fail in the service."""
    if len(schema) == 0:
        raise ValueError("The data has no columns")

    problems: List[str] = []
    for field in schema:
        arrow_type = field.type
        if pa.types.is_duration(arrow_type) or pa.types.is_time(arrow_type):
            problems.append(
                "column '{}' is of the type {}: Kusto can't ingest a timespan from a Parquet file. Cast the column to a string or to an integer "
                "(like the number of ticks of 100 nanoseconds), and convert it after the ingestion".format(field.name, arrow_type)
            )
        elif pa.types.is_null(arrow_type):
            problems.append("column '{}' is of the null type, cast it to the type of the Kusto column".format(field.name))
        elif hasattr(pa, "uuid") and arrow_type == pa.uuid() and int(pa.__version__.split(".")[0]) < _MIN_PYARROW_VERSION_FOR_UUID:
            problems.append(
                "column '{}' is of the uuid type: pyarrow {} doesn't write it the way Kusto can ingest it to a guid column, "
                "upgrade pyarrow to version {} or later".format(field.name, pa.__version__, _MIN_PYARROW_VERSION_FOR_UUID)
            )
    if problems:
        raise ValueError("The data can't be ingested to Kusto: " + "; ".join(problems))


def write_parquet(data: Any, compression: str, row_group_size: Optional[int], parquet_options: Optional[Dict[str, Any]]) -> str:
    """
    Writes Arrow data to a temporary Parquet file.
    :param data: a pyarrow Table, RecordBatch or RecordBatchReader, or an object which implements the Arrow PyCapsule stream interface.
    :return: The path of the file, which the caller has to delete.
    """
    pa, pq = _import_pyarrow()

    table = None
    batches: Any = ()
    if isinstance(data, pa.Table):
        schema, table = data.schema, data
    elif isinstance(data, pa.RecordBatch):
        schema, batches = data.schema, [data]
    elif isinstance(data, pa.RecordBatchReader):
        schema, batches = data.schema, data
    elif hasattr(data, "__arrow_c_stream__"):
        batches = pa.RecordBatchReader.from_stream(data)
        schema = batches.schema
    else:
        raise ValueError(
            "Expected a pyarrow Table, RecordBatch or RecordBatchReader, or an object which implements the Arrow PyCapsule stream interface, found {}".format(
                type(data)
            )
        )
    _check_schema(pa, schema)

    file_name = "arrow_{id}_{timestamp}_{uid}.parquet".format(id=id(data), timestamp=int(time.time()), uid=uuid.uuid4())
    file_path = os.path.join(tempfile.gettempdir(), file_name)
    options: Dict[str, Any] = {"compression": compression}
    options.update(parquet_options or {})
    try:
        with pq.ParquetWriter(file_path, schema, **options) as writer:
            if table is not None:
                writer.write_table(table, row_group_size=row_group_size)
            else:
                for batch in batches:
                    writer.write_batch(batch, row_group_size=row_group_size)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(file_path)
        raise
    return file_path
