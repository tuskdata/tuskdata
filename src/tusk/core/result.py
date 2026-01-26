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
        return {
            "columns": [{"name": c.name, "type": c.type} for c in self.columns],
            "rows": [list(row) for row in self.rows],
            "row_count": self.row_count,
            "execution_time_ms": self.execution_time_ms,
            "error": self.error,
        }
