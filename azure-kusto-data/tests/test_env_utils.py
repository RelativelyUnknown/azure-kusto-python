# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import pytest

from azure.kusto.data.env_utils import get_env


def test_get_env_returns_the_first_defined(monkeypatch):
    monkeypatch.delenv("KUSTO_TEST_FIRST", raising=False)
    monkeypatch.setenv("KUSTO_TEST_SECOND", "value")
    assert get_env("KUSTO_TEST_FIRST", "KUSTO_TEST_SECOND") == "value"


def test_get_env_raises_when_nothing_is_defined(monkeypatch):
    monkeypatch.delenv("KUSTO_TEST_MISSING", raising=False)
    with pytest.raises(ValueError):
        get_env("KUSTO_TEST_MISSING")


@pytest.mark.parametrize("default", ["", 0, "fallback"])
def test_get_env_returns_the_default_even_when_it_is_falsy(monkeypatch, default):
    monkeypatch.delenv("KUSTO_TEST_MISSING", raising=False)
    assert get_env("KUSTO_TEST_MISSING", default=default) == default
