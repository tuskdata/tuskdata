"""Query result data structures"""

from typing import Any
import msgspec


class ColumnInfo(msgspec.Struct):
    """Information about a result column"""
    name: str
    type: str


class QueryResult(msgspec.Struct):
    """Result of a query execution"""
    columns: list[ColumnInfo]
    rows: list[tuple[Any, ...]]
    row_count: int
    execution_time_ms: float
    error: str | None = None
    total_count: int | None = None  # Total rows (for pagination)
    page: int | None = None
    page_size: int | None = None

    @classmethod
    def from_error(cls, error: str) -> "QueryResult":
        """Create a result representing an error"""
        return cls(
            columns=[],
            rows=[],
            row_count=0,
            execution_time_ms=0,
            error=error
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        d = {
            "columns": [{"name": c.name, "type": c.type} for c in self.columns],
            "rows": [list(row) for row in self.rows],
            "row_count": self.row_count,
            "execution_time_ms": self.execution_time_ms,
            "error": self.error,
        }
        # Add pagination fields if present
        if self.total_count is not None:
            d["total_count"] = self.total_count
        if self.page is not None:
            d["page"] = self.page
        if self.page_size is not None:
            d["page_size"] = self.page_size
        return d
