"""Provides a hook for interacting with the Alma SRU and (future) REST API."""

from __future__ import annotations

import logging
import re
from functools import cached_property
from xml.etree import ElementTree as ET

import requests
from airflow.sdk import BaseHook

logger = logging.getLogger(__name__)

_MMSID_RE = re.compile(r"^\d{18}$")
_MARC_NS = "http://www.loc.gov/MARC21/slim"
_SRU_NS = "http://www.loc.gov/zing/srw/"
_SRU_NS_DIAG = "http://www.loc.gov/zing/srw/diagnostic/"


class AlmaError(Exception):
    """Base exception for Alma provider errors."""


class AlmaConfigurationError(AlmaError, ValueError):
    """Raised when Alma provider configuration is invalid."""


class AlmaResponseError(AlmaError, ValueError):
    """Raised when Alma returns an invalid or unexpected SRU response."""


class AlmaValidationError(AlmaError, ValueError):
    """Raised when a validation error is encountered."""


class AlmaHook(BaseHook):
    """
    Interact with the Alma SRU endpoint.

    Authentication is not yet wired in; the connection ``host`` should be the
    SRU base URL, e.g.
    ``https://berkeley.alma.exlibrisgroup.com/view/sru/01UCS_BER``.
    Credentials stored on the connection will be applied when the Alma REST
    API integration is added.

    :param conn_id: Airflow Connection ID for the Alma instance.
    """

    conn_type = "alma"
    conn_name_attr = "conn_id"
    default_conn_name = "alma_default"
    hook_name = "Alma"

    def __init__(self, conn_id: str = "alma_default") -> None:
        super().__init__()
        self.conn_id = conn_id

    def get_conn(self) -> requests.Session:
        """Return a requests session for the Alma endpoint.

        :returns: An unauthenticated :class:`requests.Session`.
        :rtype: requests.Session
        """
        return requests.Session()

    @cached_property
    def conn(self) -> requests.Session:
        """Return a cached requests session."""
        return self.get_conn()

    @cached_property
    def base_url(self) -> str:
        """Return the configured Alma SRU base URL.

        :returns: The Alma SRU base URL.
        :rtype: str
        :raises AlmaConfigurationError: If the connection host is not configured.
        """
        connection = self.get_connection(self.conn_id)
        if not connection.host:
            raise AlmaConfigurationError("Alma connection host is not configured")
        return connection.host

    def test_connection(self) -> tuple[bool, str]:
        """Test reachability of the Alma SRU endpoint via an explain request.

        :returns: A (success, message) tuple.
        :rtype: tuple[bool, str]
        """
        try:
            params = {"version": "1.2", "operation": "explain"}
            response = self.conn.get(self.base_url, params=params, timeout=10)
            if response.ok:
                return True, "Connection successful"
            return False, f"SRU explain returned {response.status_code}"
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return False, str(exc)

    def get_record_by_mms_id(self, mmsid: str) -> str:
        """Return a single MARC XML record from the *mmsid*.

        :param mmsid: An 18-digit Alma MMSID.
        :returns: The MARC XML record as a string.
        :rtype: str
        :raises AlmaValidationError: If *mmsid* is not a valid 18-digit MMSID.
        :raises AlmaResponseError: If the Alma SRU response is invalid or unexpected.
        """
        if not _MMSID_RE.match(mmsid):
            raise AlmaValidationError(f"Invalid MMSID: {mmsid!r}")

        params = {
            "version": "1.2",
            "operation": "searchRetrieve",
            "query": f"alma.mms_id={mmsid}",
            "recordSchema": "marcxml",
        }
        response = self.conn.get(self.base_url, params=params, timeout=15)
        response.raise_for_status()

        return _first_marc_record(response.text)


def _first_marc_record(sru_xml: str) -> str:
    """Return the first MARC record from an Alma SRU response.

    :param sru_xml: Raw SRU response XML string.
    :returns: The first MARC XML record as a string.
    :rtype: str
    :raises AlmaResponseError: If the XML cannot be parsed, contains no MARC
        record, contains an diagnostic message, contains zero numberOfRecords,
        or contains more than one numberOfRecords.
    """
    try:
        root = ET.fromstring(sru_xml)
    except ET.ParseError as exc:
        raise AlmaResponseError(f"Could not parse Alma SRU response: {exc}") from exc

    diagnostics = root.find(f".//{{{_SRU_NS}}}diagnostics")
    if diagnostics is not None:
        diag = diagnostics.find(f".//{{{_SRU_NS_DIAG}}}diagnostic")
        uri = diag.findtext(f".//{{{_SRU_NS_DIAG}}}uri") if diag is not None else None
        message = (
            diag.findtext(f".//{{{_SRU_NS_DIAG}}}message") if diag is not None else None
        )
        if uri is None or message is None:
            raise AlmaResponseError("Alma SRU unspecified response error")
        raise AlmaResponseError(f"Alma SRU error {uri}: {message}")

    number_text = root.findtext(f".//{{{_SRU_NS}}}numberOfRecords")
    if number_text is None:
        raise AlmaResponseError("Missing numberOfRecords")

    try:
        number_of_records = int(number_text)
    except ValueError as exc:
        raise AlmaResponseError(f"Invalid numberOfRecords: {number_text!r}") from exc
    if number_of_records == 0:
        raise AlmaResponseError("Alma SRU response contains zero records")
    if number_of_records != 1:
        raise AlmaResponseError(f"Alma SRU returned {number_of_records} records. Expected 1.")

    marc_record = root.find(f".//{{{_MARC_NS}}}record")
    if marc_record is None:
        raise AlmaResponseError("Alma SRU response contains no MARC record element")

    return ET.tostring(marc_record, encoding="unicode")
