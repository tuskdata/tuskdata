"""Tests for the Ibis-backed pipeline engine."""

import os
import tempfile
import pytest

try:
    from tusk.engines import ibis_engine
    HAS_IBIS = ibis_engine.HAS_IBIS
except ImportError:
    HAS_IBIS = False

pytestmark = pytest.mark.skipif(not HAS_IBIS, reason="ibis-framework not installed")


@pytest.fixture
def sample_csv():
    path = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False).name
    with open(path, "w") as f:
        f.write("id,name,age,salary,dept\n")
        f.write("1,alice,30,50000,eng\n")
        f.write("2,bob,25,45000,eng\n")
        f.write("3,carol,40,80000,sales\n")
        f.write("4,dave,35,60000,sales\n")
        f.write("5,eve,28,55000,eng\n")
    yield path
    os.unlink(path)


def _pipeline(sample_csv, transforms):
    from tusk.engines.polars_engine import Pipeline, DataSource
    return Pipeline(
        id="test",
        name="test",
        sources=[DataSource(id="s1", name="people", source_type="csv", path=sample_csv)],
        transforms=transforms,
        output_source_id="s1",
    )


def test_filter_and_select(sample_csv):
    from tusk.engines.polars_engine import FilterTransform, SelectTransform
    from tusk.engines.ibis_engine import execute_pipeline

    pipeline = _pipeline(sample_csv, [
        FilterTransform(column="age", operator="gte", value=30),
        SelectTransform(columns=["id", "name", "age"]),
    ])
    df = execute_pipeline(pipeline)
    assert df.shape == (3, 3)
    assert set(df.columns) == {"id", "name", "age"}


def test_group_by_with_mean(sample_csv):
    from tusk.engines.polars_engine import GroupByTransform
    from tusk.engines.ibis_engine import execute_pipeline

    pipeline = _pipeline(sample_csv, [
        GroupByTransform(by=["dept"], aggregations=[
            {"column": "salary", "agg": "mean", "alias": "avg_salary"},
            {"column": "id", "agg": "count", "alias": "n"},
        ]),
    ])
    df = execute_pipeline(pipeline)
    assert df.shape == (2, 3)
    assert "avg_salary" in df.columns
    assert "n" in df.columns


def test_case_when(sample_csv):
    from tusk.engines.ibis_engine import execute_pipeline, CaseWhenTransform, CaseWhenBranch

    pipeline = _pipeline(sample_csv, [
        CaseWhenTransform(
            alias="bucket",
            branches=[
                CaseWhenBranch(column="age", operator="gte", value=40, result="senior"),
                CaseWhenBranch(column="age", operator="gte", value=30, result="mid"),
            ],
            default="junior",
        ),
    ])
    df = execute_pipeline(pipeline)
    assert "bucket" in df.columns
    buckets = set(df["bucket"].to_list())
    assert buckets == {"senior", "mid", "junior"}


def test_profile(sample_csv):
    from tusk.engines.ibis_engine import profile

    from tusk.engines.polars_engine import Pipeline, DataSource
    pipeline = Pipeline(
        id="test", name="test",
        sources=[DataSource(id="s1", name="people", source_type="csv", path=sample_csv)],
        transforms=[],
        output_source_id="s1",
    )
    result = profile(pipeline)
    assert "columns" in result
    by_name = {c["name"]: c for c in result["columns"]}

    assert by_name["age"]["rows"] == 5
    assert by_name["age"]["null_count"] == 0
    assert by_name["age"]["distinct"] == 5
    assert by_name["age"]["min"] == 25
    assert by_name["age"]["max"] == 40


def test_sort_and_limit(sample_csv):
    from tusk.engines.polars_engine import SortTransform, LimitTransform
    from tusk.engines.ibis_engine import execute_pipeline

    pipeline = _pipeline(sample_csv, [
        SortTransform(columns=["age"], descending=[True]),
        LimitTransform(n=2),
    ])
    df = execute_pipeline(pipeline)
    ages = df["age"].to_list()
    assert ages == [40, 35]


def test_distinct(sample_csv):
    from tusk.engines.polars_engine import DistinctTransform, SelectTransform
    from tusk.engines.ibis_engine import execute_pipeline

    pipeline = _pipeline(sample_csv, [
        SelectTransform(columns=["dept"]),
        DistinctTransform(),
    ])
    df = execute_pipeline(pipeline)
    assert df.shape[0] == 2


def test_missing_ibis_is_clear():
    """If HAS_IBIS is False, execute_pipeline should raise a helpful error,
    not a NameError."""
    import tusk.engines.ibis_engine as mod
    saved = mod.HAS_IBIS
    mod.HAS_IBIS = False
    try:
        with pytest.raises(RuntimeError, match="ibis-framework"):
            mod._require_ibis()
    finally:
        mod.HAS_IBIS = saved
