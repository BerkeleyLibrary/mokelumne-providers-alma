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
_LANG_CODE_RE = re.compile(r"[a-z]{3}")

_SRW_NS = "http://www.loc.gov/zing/srw/"
_MARC_NS = "http://www.loc.gov/MARC21/slim"
# 041 subfields that carry language codes
_LANG_SUBFIELDS = ("a", "b", "e", "f", "g")


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

    def get_marc_language_codes(self, mmsid: str) -> list[str]:
        """Return MARC language codes for the record identified by *mmsid*.

        Reads from the 008 fixed field (positions 35–37) and from all
        subfields a/b/e/f/g of every 041 field.  Codes are deduplicated
        and returned in the order first encountered, consistent with the
        Perl implementation this replaces.

        :param mmsid: An 18-digit Alma MMSID.
        :returns: Unique three-letter MARC language codes, or an empty list
            when no record is found.
        :rtype: list[str]
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

        return _extract_language_codes(response.text)


def _extract_language_codes(sru_xml: str) -> list[str]:
    """Parse an Alma SRU marcxml response and return MARC language codes.

    :param sru_xml: Raw SRU response XML string.
    :returns: Unique three-letter MARC language codes.
    :rtype: list[str]
    :raises AirflowException: If the XML cannot be parsed or contains
        more than one record.
    """
    try:
        root = ET.fromstring(sru_xml)
    except ET.ParseError as exc:
        raise AirflowException(f"Could not parse Alma SRU response: {exc}") from exc

    num_el = root.find(f"{{{_SRW_NS}}}numberOfRecords")
    if num_el is None or not num_el.text:
        raise AirflowException("Alma SRU response missing numberOfRecords")
    num = int(num_el.text.strip())
    if num == 0:
        return []
    if num > 1:
        raise AirflowException(f"Alma SRU returned {num} records for MMSID; expected 1")

    marc_record = root.find(f".//{{{_MARC_NS}}}record")
    if marc_record is None:
        raise AirflowException("Alma SRU response contains no MARC record element")

    codes: list[str] = []

    cf008 = marc_record.find(f"{{{_MARC_NS}}}controlfield[@tag='008']")
    if cf008 is not None and cf008.text and len(cf008.text) >= 38:
        lang = cf008.text[35:38]
        if re.fullmatch(r"[a-z]{3}", lang):
            codes.append(lang)

    for f041 in marc_record.findall(f"{{{_MARC_NS}}}datafield[@tag='041']"):
        combined = ""
        for sf_code in _LANG_SUBFIELDS:
            for subfield in f041.findall(f"{{{_MARC_NS}}}subfield[@code='{sf_code}']"):
                combined += subfield.text or ""
        for m in _LANG_CODE_RE.finditer(combined):
            lang = m.group()
            if lang not in codes:
                codes.append(lang)

    return codes
