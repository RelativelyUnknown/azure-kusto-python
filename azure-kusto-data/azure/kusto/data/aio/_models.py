from typing import AsyncIterator

from azure.kusto.data._models import KustoResultRow, BaseStreamingKustoResultTable


class KustoStreamingResultTable(BaseStreamingKustoResultTable):
    """Async Iterator over a Kusto result table."""

    async def __anext__(self) -> KustoResultRow:
        try:
            row = await self.raw_rows.__anext__()
        except StopAsyncIteration:
            self.finished = True
            raise
        self.row_count += 1
        return KustoResultRow(self.columns, row)

    def __aiter__(self) -> AsyncIterator[KustoResultRow]:
        return self

    async def _aiter_raw_rows(self) -> AsyncIterator[list]:
        """
        Iterates over the raw (unconverted) rows, while keeping `row_count` and `finished` up to date,
        so that the response data set can advance to the next table once this one is exhausted.
        """
        async for row in self.raw_rows:
            self.row_count += 1
            yield row
        self.finished = True
