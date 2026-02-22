"""Tests for _safe_eval_polars_expr() — the most security-sensitive function in the codebase.

This function parses user-provided expression strings into Polars expressions
using AST parsing. It MUST reject any attempt to execute arbitrary code.
"""

import pytest
import polars as pl

from tusk.engines.polars_engine import _safe_eval_polars_expr


class TestValidExpressions:
    """Expressions that should parse successfully."""

    def test_simple_column(self):
        expr = _safe_eval_polars_expr("pl.col('name')")
        assert expr is not None

    def test_comparison_gt(self):
        expr = _safe_eval_polars_expr("pl.col('age') > 18")
        assert expr is not None

    def test_comparison_eq(self):
        expr = _safe_eval_polars_expr("pl.col('status') == 'active'")
        assert expr is not None

    def test_literal(self):
        expr = _safe_eval_polars_expr("pl.lit(42)")
        assert expr is not None

    def test_arithmetic(self):
        expr = _safe_eval_polars_expr("pl.col('price') * pl.col('quantity')")
        assert expr is not None

    def test_string_method(self):
        expr = _safe_eval_polars_expr("pl.col('name').str.to_uppercase()")
        assert expr is not None

    def test_round(self):
        expr = _safe_eval_polars_expr("pl.col('value').round(2)")
        assert expr is not None

    def test_alias(self):
        expr = _safe_eval_polars_expr("pl.col('x').alias('y')")
        assert expr is not None

    def test_is_null(self):
        expr = _safe_eval_polars_expr("pl.col('x').is_null()")
        assert expr is not None

    def test_fill_null(self):
        expr = _safe_eval_polars_expr("pl.col('x').fill_null(0)")
        assert expr is not None

    def test_cast(self):
        expr = _safe_eval_polars_expr("pl.col('x').cast(pl.Int64)")
        assert expr is not None

    def test_negation(self):
        expr = _safe_eval_polars_expr("-pl.col('x')")
        assert expr is not None


class TestRejectedExpressions:
    """Expressions that MUST be rejected for security."""

    def test_rejects_import(self):
        with pytest.raises(ValueError):
            _safe_eval_polars_expr("__import__('os').system('rm -rf /')")

    def test_rejects_eval(self):
        with pytest.raises(ValueError):
            _safe_eval_polars_expr("eval('malicious')")

    def test_rejects_exec(self):
        with pytest.raises(ValueError):
            _safe_eval_polars_expr("exec('malicious')")

    def test_rejects_dunder_access(self):
        with pytest.raises(ValueError):
            _safe_eval_polars_expr("pl.col('x').__class__.__bases__")

    def test_rejects_open(self):
        with pytest.raises(ValueError):
            _safe_eval_polars_expr("open('/etc/passwd')")

    def test_rejects_os_module(self):
        with pytest.raises(ValueError):
            _safe_eval_polars_expr("os.system('whoami')")

    def test_rejects_subprocess(self):
        with pytest.raises(ValueError):
            _safe_eval_polars_expr("subprocess.run(['ls'])")

    def test_rejects_arbitrary_name(self):
        with pytest.raises(ValueError):
            _safe_eval_polars_expr("some_variable")

    def test_rejects_disallowed_method(self):
        with pytest.raises(ValueError):
            _safe_eval_polars_expr("pl.col('x').apply(lambda x: x)")

    def test_rejects_invalid_syntax(self):
        with pytest.raises(ValueError):
            _safe_eval_polars_expr("this is not python")
