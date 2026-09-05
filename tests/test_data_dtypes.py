"""Column-type labels shown in the Data preview stay short."""

import polars as pl

from tusk.engines.polars_engine import short_dtype


def test_short_dtype_labels():
    assert short_dtype(pl.Int64) == "Int64"
    assert short_dtype(pl.Decimal(38, 2)) == "Decimal(38,2)"
    assert short_dtype(pl.Datetime("us", "UTC")) == "Datetime[us, UTC]"
    assert short_dtype(pl.Datetime("ms")) == "Datetime[ms]"
    assert short_dtype(pl.Duration("ns")) == "Duration[ns]"
    assert short_dtype(pl.List(pl.Decimal(10, 3))) == "List[Decimal(10,3)]"
