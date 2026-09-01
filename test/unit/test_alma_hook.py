"""Pytest testcases for `mokelumne.providers.alma.hooks.alma`."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from mokelumne.providers.alma.hooks.alma import (
    AlmaHook,
    AlmaConfigurationError,
    AlmaResponseError,
    AlmaValidationError,
)

_FIXTURES = Path(__file__).parent.parent / "fixtures"


class DummyConnection:
    """Mock Airflow connection."""

    host = "https://berkeley.alma.exlibrisgroup.com/view/sru/01UCS_BER"
    login = "dummy-api-key"
    password = None


def test_get_record_by_mms_id_returns_first_marc_record_from_fixture():
    """Verify MARC record extraction against a real Alma SRU fixture response."""
    fixture_xml = (_FIXTURES / "991073999959706532.xml").read_text()

    mock_response = MagicMock(spec=requests.Response)
    mock_response.ok = True
    mock_response.text = fixture_xml

    mock_session = MagicMock(spec=requests.Session)
    mock_session.get.return_value = mock_response

    hook = AlmaHook()
    hook.get_connection = MagicMock(return_value=DummyConnection())
    hook.__dict__["conn"] = mock_session  # bypass cached_property

    record_xml = hook.get_record_by_mms_id("991073999959706532")

    assert record_xml.startswith(
        '<ns0:record xmlns:ns0="http://www.loc.gov/MARC21/slim">'
    )
    assert "<ns0:controlfield tag=\"001\">991073999959706532</ns0:controlfield>" in record_xml
    assert "searchRetrieveResponse" not in record_xml
    call_url = mock_session.get.call_args[0][0]
    assert "991073999959706532" in call_url
