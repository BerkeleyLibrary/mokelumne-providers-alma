"""Pytest testcases for `mokelumne.providers.alma.hooks.alma`."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
import requests

from mokelumne.providers.alma.hooks.alma import (
    AlmaHook,
    AlmaConfigurationError,
    AlmaResponseError,
    AlmaValidationError,
    _first_marc_record,
)

_FIXTURES = Path(__file__).parent.parent / "fixtures"
_BASE_URL = "https://berkeley.alma.exlibrisgroup.com/view/sru/01UCS_BER"


class DummyConnection:
    """Mock Airflow connection."""

    host = _BASE_URL
    login = "dummy-api-key"
    password = None


@pytest.fixture(name="mock_response")
def fixture_mock_response() -> MagicMock:
    """Return a mocked HTTP response."""
    response = MagicMock(spec=requests.Response)
    response.ok = True
    response.status_code = 200
    return response


@pytest.fixture(name="mock_session")
def fixture_mock_session(mock_response: MagicMock) -> MagicMock:
    """Return a mocked requests session."""
    session = MagicMock(spec=requests.Session)
    session.get.return_value = mock_response
    return session


@pytest.fixture(name="hook")
def fixture_hook(mock_session: MagicMock) -> AlmaHook:
    """Return an Alma hook with mocked Airflow connection and HTTP session."""
    alma_hook = AlmaHook()
    cast(Any, alma_hook).get_connection = MagicMock(return_value=DummyConnection())
    alma_hook.__dict__["conn"] = mock_session
    return alma_hook


def fixture_xml(name: str) -> str:
    """Return an Alma SRU XML fixture."""
    return (_FIXTURES / name).read_text(encoding="utf-8")


def test_get_conn_returns_requests_session() -> None:
    """Verify `get_conn` creates a requests session."""
    mock_session = MagicMock(spec=requests.Session)

    with patch("mokelumne.providers.alma.hooks.alma.requests.Session") as session_cls:
        session_cls.return_value = mock_session

        assert AlmaHook().get_conn() is mock_session


def test_base_url_returns_connection_host(hook: AlmaHook) -> None:
    """Verify base URL configuration is read from the Airflow connection."""
    assert hook.base_url == _BASE_URL
    cast(MagicMock, hook.get_connection).assert_called_once_with("alma_default")


def test_base_url_raises_alma_configuration_error_when_host_missing() -> None:
    """Verify a missing connection host raises an Alma configuration error."""
    hook = AlmaHook(conn_id="missing_host")
    cast(Any, hook).get_connection = MagicMock(return_value=SimpleNamespace(host=""))

    with pytest.raises(AlmaConfigurationError, match="host is not configured"):
        hook.base_url


def test_test_connection_returns_success_for_ok_explain(
    hook: AlmaHook, mock_session: MagicMock, mock_response: MagicMock
) -> None:
    """Verify `test_connection` reports success for a reachable SRU endpoint."""
    mock_response.ok = True

    assert hook.test_connection() == (True, "Connection successful")
    mock_session.get.assert_called_once_with(
        _BASE_URL,
        params={"version": "1.2", "operation": "explain"},
        timeout=10,
    )


def test_test_connection_returns_status_message_for_failed_explain(
    hook: AlmaHook, mock_response: MagicMock
) -> None:
    """Verify `test_connection` reports the Alma SRU status code."""
    mock_response.ok = False
    mock_response.status_code = 503

    assert hook.test_connection() == (False, "SRU explain returned 503")


def test_test_connection_returns_exception_message(
    hook: AlmaHook, mock_session: MagicMock
) -> None:
    """Verify `test_connection` catches request failures."""
    mock_session.get.side_effect = requests.RequestException("connection failed")

    assert hook.test_connection() == (False, "connection failed")


def test_get_record_by_mms_id_returns_first_marc_record_from_fixture(
    hook: AlmaHook, mock_session: MagicMock, mock_response: MagicMock
) -> None:
    """Verify MARC record extraction against a real Alma SRU fixture response."""
    mock_response.text = fixture_xml("991073999959706532.xml")

    record_xml = hook.get_record_by_mms_id("991073999959706532")

    mock_session.get.assert_called_once_with(
        _BASE_URL,
        params={
            "version": "1.2",
            "operation": "searchRetrieve",
            "query": "alma.mms_id=991073999959706532",
            "recordSchema": "marcxml",
        },
        timeout=15,
    )
    mock_response.raise_for_status.assert_called_once_with()
    assert record_xml.startswith(
        '<ns0:record xmlns:ns0="http://www.loc.gov/MARC21/slim">'
    )
    assert "<ns0:controlfield tag=\"001\">991073999959706532</ns0:controlfield>" in record_xml
    assert "searchRetrieveResponse" not in record_xml


@pytest.mark.parametrize("mmsid", ["", "abc", "99107399995970653", "9910739999597065320"])
def test_get_record_by_mms_id_raises_alma_validation_error_for_invalid_mmsid(
    hook: AlmaHook, mock_session: MagicMock, mmsid: str
) -> None:
    """Verify invalid MMSIDs are rejected before making an HTTP request."""
    with pytest.raises(AlmaValidationError, match="Invalid MMSID"):
        hook.get_record_by_mms_id(mmsid)

    mock_session.get.assert_not_called()


def test_get_record_by_mms_id_raises_http_error(
    hook: AlmaHook, mock_response: MagicMock
) -> None:
    """Verify HTTP response errors are propagated."""
    mock_response.raise_for_status.side_effect = requests.HTTPError("bad response")

    with pytest.raises(requests.HTTPError, match="bad response"):
        hook.get_record_by_mms_id("991073999959706532")


@pytest.mark.parametrize(
    ("xml", "message"),
    [
        (fixture_xml("error.xml"), "Alma SRU error 200801: Catalog search"),
        (fixture_xml("zero_records.xml"), "zero records"),
        ("not xml", "Could not parse Alma SRU response"),
        (
            '<searchRetrieveResponse xmlns="http://www.loc.gov/zing/srw/" />',
            "Missing numberOfRecords",
        ),
        (
            (
                '<searchRetrieveResponse xmlns="http://www.loc.gov/zing/srw/">'
                "<numberOfRecords>many</numberOfRecords>"
                "</searchRetrieveResponse>"
            ),
            "Invalid numberOfRecords: 'many'",
        ),
        (
            (
                '<searchRetrieveResponse xmlns="http://www.loc.gov/zing/srw/">'
                "<numberOfRecords>2</numberOfRecords>"
                "</searchRetrieveResponse>"
            ),
            "Alma SRU returned 2 records. Expected 1.",
        ),
        (
            (
                '<searchRetrieveResponse xmlns="http://www.loc.gov/zing/srw/">'
                "<numberOfRecords>1</numberOfRecords>"
                "</searchRetrieveResponse>"
            ),
            "contains no MARC record element",
        ),
    ],
)
def test_first_marc_record_raises_alma_response_error(xml: str, message: str) -> None:
    """Verify invalid Alma SRU responses raise clear response errors."""
    with pytest.raises(AlmaResponseError, match=message):
        _first_marc_record(xml)
