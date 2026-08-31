"""Provides a hook for interacting with the Alma SRU and (future) REST API."""

from __future__ import annotations

import logging
import re
from functools import cached_property
from urllib.parse import urlencode
from xml.etree import ElementTree as ET

import requests
from airflow.sdk import BaseHook
from airflow.sdk.exceptions import AirflowException

logger = logging.getLogger(__name__)

_MMSID_RE = re.compile(r"^\d{18}$")
_MARC_NS = "http://www.loc.gov/MARC21/slim"

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
        :raises AirflowException: If the connection host is not configured.
        """
        connection = self.get_connection(self.conn_id)
        if not connection.host:
            raise AirflowException("Alma connection host is not configured")
        return requests.Session()

    @cached_property
    def conn(self) -> requests.Session:
        """Return a cached requests session."""
        return self.get_conn()

    def test_connection(self) -> tuple[bool, str]:
        """Test reachability of the Alma SRU endpoint via an explain request.

        :returns: A (success, message) tuple.
        :rtype: tuple[bool, str]
        """
        try:
            connection = self.get_connection(self.conn_id)
            if not connection.host:
                return False, "Alma connection host is not configured"
            params = {"version": "1.2", "operation": "explain"}
            url = f"{connection.host}?{urlencode(params)}"
            response = self.conn.get(url, timeout=10)
            if response.ok:
                return True, "Connection successful"
            return False, f"SRU explain returned {response.status_code}"
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return False, str(exc)

    def get_record_by_mms_id(self, mmsid: str) -> str:
        """Return a single MARC XML record from the *mmsid*.

        :param str mmsid: An 18-digit Alma MMSID.
        :returns: The MARC XML record as a string.
        :rtype: str
        :raises AirflowException: If *mmsid* is invalid, the HTTP request
            fails, or the response contains more than one record.
        """
        if not _MMSID_RE.match(mmsid):
            raise AirflowException(f"Invalid MMSID: {mmsid!r}")

        connection = self.get_connection(self.conn_id)
        params = {
            "version": "1.2",
            "operation": "searchRetrieve",
            "query": f"alma.mms_id={mmsid}",
            "recordSchema": "marcxml",
        }
        url = f"{connection.host}?{urlencode(params)}"

        response = self.conn.get(url, timeout=15)
        if not response.ok:
            raise AirflowException(
                f"Alma SRU request failed: {response.status_code} {response.reason}"
            )

        return _first_marc_record(response.text)


def _first_marc_record(sru_xml: str) -> str:
    """Return the first MARC record from an Alma SRU response.

    :param str sru_xml: Raw SRU response XML string.
    :returns: The first MARC XML record as a string.
    :rtype: str
    :raises AirflowException: If the XML cannot be parsed or contains no MARC
        record.
    """
    try:
        root = ET.fromstring(sru_xml)
    except ET.ParseError as exc:
        raise AirflowException(f"Could not parse Alma SRU response: {exc}") from exc

    marc_record = root.find(f".//{{{_MARC_NS}}}record")
    if marc_record is None:
        raise AirflowException("Alma SRU response contains no MARC record element")

    return ET.tostring(marc_record, encoding="unicode")
