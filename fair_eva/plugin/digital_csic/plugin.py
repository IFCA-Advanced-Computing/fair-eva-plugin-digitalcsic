#!/usr/bin/python
# -*- coding: utf-8 -*-
import ast
import csv
import json
import logging
import os
import sys
import urllib
from functools import wraps

import fair_eva.api.utils as ut
import idutils
import pandas as pd
import psycopg2
import requests
from bs4 import BeautifulSoup
from fair_eva.api.evaluator import ConfigTerms, EvaluatorBase

logging.basicConfig(
    stream=sys.stdout, level=logging.DEBUG, format="'%(name)s:%(lineno)s' | %(message)s"
)
logger = logging.getLogger("api.plugin")


class Plugin(EvaluatorBase):
    """A class used to define FAIR indicators tests. It is tailored towards the
    DigitalCSIC repository.

    Attributes
    ----------
    item_id : str
        Digital Object identifier, which can be a generic one (DOI, PID), or an internal (e.g. an
            identifier from the repo)

    api_endpoint : str
        Open Archives Initiative , This is the place in which the API will ask for the metadata. If you are working with  Digital CSIC http://digital.csic.es/dspace-oai/request

    lang : Language
    """

    def __init__(
        self,
        item_id,
        api_endpoint="http://digital.csic.es/dspace-oai/request",
        lang="en",
        config=None,
        name="digital_csic",
    ):
        self.config = config
        self.name = name
        self.lang = lang
        self.oai_base = api_endpoint

        if ut.get_doi_str(item_id) != "":
            self.item_id = ut.get_doi_str(item_id)
            self.id_type = "doi"
        elif ut.get_handle_str(item_id) != "":
            self.item_id = ut.get_handle_str(item_id)
            self.id_type = "handle"
        else:
            self.item_id = item_id
            self.id_type = "internal"

        super().__init__(self.item_id, self.oai_base, self.lang, self.config, self.name)

        self.file_list = None
        self.metadata = self.get_metadata()
        self.metadata_schemas = ast.literal_eval(
            self.config[self.name]["metadata_schemas"]
        )

        global _
        _ = super().translation()

        if self.metadata is None or len(self.metadata) == 0:
            raise Exception(_("Problem accessing data and metadata. Please, try again"))
            # self.metadata = oai_metadata
        logger.debug("Metadata is: %s" % self.metadata)

        self.metadata_quality = 100  # Value for metadata balancing

    def get_metadata(self):
        cfg = self.config.get(self.name, {}) if isinstance(self.config, dict) else {}
        test_mode = self.config[self.name]["test_mode"]
        test_file = self.config[self.name]["test_metadata_file"]
        
        # MODE 1: TEST MODE - Load only from CSV test file
        if test_mode:
            logger.debug("TEST MODE ENABLED - Loading metadata from CSV test file only")
            if test_file:
                if not os.path.isabs(test_file):
                    test_file = os.path.join(os.path.dirname(__file__), test_file)
                if os.path.exists(test_file):
                    try:
                        if test_file.lower().endswith(".csv"):
                            self.metadata = pd.read_csv(test_file)
                            logger.debug(f"✓ Metadata loaded from test CSV: {test_file}")
                            return self.metadata
                        elif test_file.lower().endswith(".json"):
                            self.metadata = pd.read_json(test_file)
                            logger.debug(f"✓ Metadata loaded from test JSON: {test_file}")
                            return self.metadata
                    except Exception as e:
                        logger.error(f"✗ Error loading test metadata from {test_file}: {e}")
                        raise Exception(f"Failed to load test metadata: {e}")
                else:
                    raise Exception(f"Test file not found: {test_file}")
            else:
                raise Exception("test_mode=true but test_metadata_file is not configured")
        
        # MODE 2: PRODUCTION MODE - Try API or Database
        logger.debug("PRODUCTION MODE - Attempting to retrieve metadata from API or Database")
        api_metadata = None
        
        # Try API first (only for DOI or HANDLE)
        if self.id_type == "doi" or self.id_type == "handle":
            logger.debug(f"Attempting API retrieval for {self.id_type}: {self.item_id}")
            try:
                api_endpoint = "https://digital.csic.es"
                api_metadata, self.file_list = self.get_metadata_api(
                    api_endpoint, self.item_id, self.id_type
                )
                if api_metadata is not None and len(api_metadata) > 0:
                    logger.debug("✓ Metadata from API retrieved successfully")
                    self.access_protocols = ["http"]
                    self.metadata = api_metadata
                    temp_md = self.metadata.query("element == 'identifier'")
                    if len(temp_md) > 0:
                        uri_md = temp_md.query("qualifier == 'uri'")
                        if len(uri_md) > 0:
                            self.item_id = uri_md["text_value"].values[0]
                    logger.info(f"✓ API metadata retrieved and processed successfully")
                    return self.metadata
                else:
                    logger.debug("✗ API returned empty metadata")
                    api_metadata = None
            except Exception as e:
                logger.error(f"✗ Error retrieving metadata from API: {e}")
                api_metadata = None
        
        # Try Database if API failed or not applicable
        logger.debug("Attempting database connection...")
        try:
            self.connection = psycopg2.connect(
                user=self.config["digital_csic"]["db_user"],
                password=self.config["digital_csic"]["db_pass"],
                host=self.config["digital_csic"]["db_host"],
                port=self.config["digital_csic"]["db_port"],
                database=self.config["digital_csic"]["db_db"],
            )
            logger.debug("✓ Database connection established")
            
            self.internal_id = self.get_internal_id(self.item_id, self.connection)
            if self.id_type == "doi":
                self.handle_id = self.get_handle_id(
                    self.internal_id, self.connection
                )
            elif self.id_type == "internal":
                self.handle_id = self.get_handle_id(
                    self.internal_id, self.connection
                )
                self.item_id = self.handle_id

            logger.debug(f"INTERNAL ID: {self.internal_id} ITEM ID: {self.item_id}")

            self.metadata = self.get_metadata_db()
            logger.debug("✓ Metadata retrieved from database successfully")
            self.metadata.to_csv("metadata_testing.csv")
            return self.metadata
            
        except Exception as e:
            logger.error(f"✗ Error connecting to database: {type(e).__name__}: {e}")
            raise Exception(f"Failed to retrieve metadata from API or Database: {e}")

    def get_metadata_api(self, api_endpoint, item_pid, item_type):
        if item_type == "doi":
            md_key = "dc.identifier.doi"
            item_pid = idutils.to_url(item_pid, item_type, "https")
        elif item_type == "handle":
            md_key = "dc.identifier.uri"
            item_pid = ut.pid_to_url(item_pid, item_type)

        try:
            logger.debug("get_metadata_api IMPORTANT: %s" % item_pid)
            data = {"key": md_key, "value": item_pid}
            headers = {"accept": "application/json", "Content-Type": "application/json"}
            logger.debug("get_metadata_api to POST: %s" % data)
            url = api_endpoint + "/rest/items/find-by-metadata-field"
            logger.debug("get_metadata_api POST / %s" % url)
            MAX_RETRIES = 5
            for _ in range(MAX_RETRIES):
                r = requests.post(
                    url,
                    data=json.dumps(data),
                    headers=headers,
                    verify=False,
                    timeout=15,
                )
                if r.status_code == 200:
                    break
            if len(r.text) == 2:
                data = {"key": md_key, "value": idutils.normalize_doi(item_pid)}
                for _ in range(MAX_RETRIES):
                    r = requests.post(
                        url,
                        data=json.dumps(data),
                        headers=headers,
                        verify=False,
                        timeout=15,
                    )
                    if r.status_code == 200:
                        break
            logger.debug("get_metadata_api ID FOUND: %s" % r.text)
            if r.status_code == 200:
                item_id = r.json()[0]["id"]
                url = api_endpoint + "/rest/items/%s/metadata" % item_id
                for _ in range(MAX_RETRIES):
                    r = requests.get(url, headers=headers, verify=False, timeout=15)
                    if r.status_code == 200:
                        break
            else:
                logger.error(
                    "get_metadata_api Request to URL: %s failed with STATUS: %i"
                    % (url, r.status_code)
                )
            md = []
            for e in r.json():
                split_term = e["key"].split(".")
                metadata_schema = self.metadata_prefix_to_uri(split_term[0])
                element = split_term[1]
                if len(split_term) > 2:
                    qualifier = split_term[2]
                else:
                    qualifier = ""
                text_value = e["value"]
                md.append([text_value, metadata_schema, element, qualifier])
            metadata = pd.DataFrame(
                md, columns=["text_value", "metadata_schema", "element", "qualifier"]
            )
            file_list = pd.DataFrame(
                self.resolve_item_files(item_pid, item_type),
                columns=["name", "extension", "format", "link", "final_url"],
            )
        except Exception as e:
            logger.error(
                "get_metadata_api Problem creating Metadata from API: %s when calling URL"
                % e
            )
            metadata = []
            file_list = []
        return metadata, file_list

    def resolve_item_files(self, item_pid, item_type=None, api_endpoint="https://digital.csic.es"):
        """Resolve DOI/handle in DIGITAL.CSIC and return attached files.

        Returns a list of dicts with at least:
          - name: filename
          - final_url: final URL after redirects
        Extra fields are kept for plugin compatibility:
          - extension
          - format
          - link (bitstream endpoint URL)
        """
        if not item_pid:
            return []

        if item_type is None:
            if ut.get_doi_str(item_pid) != "":
                item_type = "doi"
                item_pid = ut.get_doi_str(item_pid)
            elif ut.get_handle_str(item_pid) != "":
                item_type = "handle"
                item_pid = ut.get_handle_str(item_pid)
            else:
                raise ValueError("Identifier must be a DOI or handle")

        if item_type == "doi":
            md_key = "dc.identifier.doi"
            md_value = idutils.to_url(item_pid, "doi", "https")
            identifier_url = md_value
        elif item_type == "handle":
            md_key = "dc.identifier.uri"
            md_value = ut.pid_to_url(item_pid, "handle")
            identifier_url = md_value
        else:
            raise ValueError("item_type must be 'doi' or 'handle'")

        # 1) First attempt: Signposting on DOI/handle landing page.
        files = self._resolve_item_files_signposting(identifier_url)
        if len(files) > 0:
            return files

        # 2) Fallback: DIGITAL.CSIC REST API.
        headers = {"accept": "application/json", "Content-Type": "application/json"}
        find_url = api_endpoint + "/rest/items/find-by-metadata-field"
        item_id = None

        payloads = [{"key": md_key, "value": md_value}]
        if item_type == "doi":
            payloads.append({"key": md_key, "value": idutils.normalize_doi(md_value)})

        for payload in payloads:
            try:
                response = requests.post(
                    find_url,
                    data=json.dumps(payload),
                    headers=headers,
                    verify=False,
                    timeout=15,
                )
                if response.status_code != 200:
                    continue
                items = response.json()
                if len(items) > 0:
                    item_id = items[0].get("id")
                    break
            except Exception as e:
                logger.error("resolve_item_files error resolving item id: %s", e)

        if not item_id:
            return []

        bitstreams_url = api_endpoint + "/rest/items/%s/bitstreams" % item_id
        try:
            response = requests.get(
                bitstreams_url, headers=headers, verify=False, timeout=15
            )
            if response.status_code != 200:
                return []
            bitstreams = response.json()
        except Exception as e:
            logger.error("resolve_item_files error getting bitstreams: %s", e)
            return []

        files = []
        for bitstream in bitstreams:
            link = api_endpoint + bitstream.get("link", "")
            final_url = link
            try:
                resolved = requests.head(
                    link, allow_redirects=True, verify=False, timeout=15
                )
                if resolved.status_code == 405:
                    resolved = requests.get(
                        link, allow_redirects=True, verify=False, timeout=15, stream=True
                    )
                if resolved.url:
                    final_url = resolved.url
            except Exception:
                pass

            name = bitstream.get("name", "")
            files.append(
                {
                    "name": name,
                    "extension": name.split(".")[-1] if "." in name else "",
                    "format": bitstream.get("format", ""),
                    "link": link,
                    "final_url": final_url,
                }
            )

        return files

    def _resolve_item_files_signposting(self, identifier_url):
        files = []
        seen_urls = set()
        link_headers = []

        try:
            response = requests.head(
                identifier_url, allow_redirects=True, verify=False, timeout=15
            )
            if response.status_code == 405:
                response = requests.get(
                    identifier_url,
                    allow_redirects=True,
                    verify=False,
                    timeout=15,
                    stream=True,
                )

            raw_headers = None
            if hasattr(response, "raw") and hasattr(response.raw, "headers"):
                raw_headers = response.raw.headers

            if raw_headers is not None:
                if hasattr(raw_headers, "get_all"):
                    link_headers.extend(raw_headers.get_all("Link") or [])
                elif hasattr(raw_headers, "getlist"):
                    link_headers.extend(raw_headers.getlist("Link") or [])

            if response.headers.get("Link"):
                link_headers.append(response.headers.get("Link"))
        except Exception as e:
            logger.error("resolve_item_files signposting request error: %s", e)
            return []

        for header_value in link_headers:
            try:
                parsed_links = requests.utils.parse_header_links(
                    header_value.replace(">,<", ">, <")
                )
            except Exception:
                parsed_links = []
            for link_info in parsed_links:
                rel = (link_info.get("rel") or "").strip().lower()
                if "item" not in rel.split():
                    continue

                item_url = link_info.get("url", "")
                if not item_url or item_url in seen_urls:
                    continue
                seen_urls.add(item_url)

                final_url = item_url
                try:
                    resolved = requests.head(
                        item_url, allow_redirects=True, verify=False, timeout=15
                    )
                    if resolved.status_code == 405:
                        resolved = requests.get(
                            item_url,
                            allow_redirects=True,
                            verify=False,
                            timeout=15,
                            stream=True,
                        )
                    if resolved.url:
                        final_url = resolved.url
                except Exception:
                    pass

                path = urllib.parse.urlparse(final_url).path
                name = urllib.parse.unquote(path.split("/")[-1]) if path else ""
                files.append(
                    {
                        "name": name,
                        "extension": name.split(".")[-1] if "." in name else "",
                        "format": link_info.get("type", ""),
                        "link": item_url,
                        "final_url": final_url,
                    }
                )

        return files

    def get_metadata_db(self):
        query = (
            "SELECT metadatavalue.text_value, metadataschemaregistry.short_id, metadatafieldregistry.element,\
                metadatafieldregistry.qualifier FROM item, metadatavalue, metadataschemaregistry, metadatafieldregistry WHERE item.item_id = %s and \
    item.item_id = metadatavalue.resource_id AND metadatavalue.metadata_field_id = metadatafieldregistry.metadata_field_id \
    AND metadatafieldregistry.metadata_schema_id = metadataschemaregistry.metadata_schema_id AND resource_type_id = 2"
            % self.internal_id
        )
        cursor = self.connection.cursor()
        cursor.execute(query)
        metadata = pd.DataFrame(
            cursor.fetchall(),
            columns=["text_value", "metadata_schema", "element", "qualifier"],
        )
        for i in range(len(metadata["metadata_schema"])):
            metadata["metadata_schema"][i] = self.metadata_prefix_to_uri(
                metadata["metadata_schema"][i]
            )
        return metadata

        # TESTS

    # ACCESS
    @ConfigTerms(term_id="terms_access")
    def rda_a1_01m(self, **kwargs):
        """Indicator RDA-A1-01M.

        This indicator is linked to the following principle: A1: (Meta)data are retrievable by their
        identifier using a standardised communication protocol. More information about that
        principle can be found here.

        The indicator refers to the information that is necessary to allow the requester to gain access
        to the digital object. It is (i) about whether there are restrictions to access the data (i.e.
        access to the data may be open, restricted or closed), (ii) the actions to be taken by a
        person who is interested to access the data, in particular when the data has not been
        published on the Web and (iii) specifications that the resources are available through
        eduGAIN7 or through specialised solutions such as proposed for EPOS.

        Returns
        -------
        points
            - 100 if access metadata is available and data can be access manually
            - 0 otherwise
        msg
            Message with the results or recommendations to improve this indicator
        """
        # 1 - Check metadata record for access info
        msg_list = []
        points = 0

        term_data = self._get_config_term_from_kwargs(kwargs, "terms_access")
        term_metadata = term_data["metadata"]

        msg_st_list = []
        for index, row in term_metadata.iterrows():
            msg_st_list.append(
                _("Metadata found for access") + ": " + row["text_value"]
            )
            logging.debug(_("Metadata found for access") + ": " + row["text_value"])
            points = 100
        msg_list.append({"message": msg_st_list, "points": points})

        # 2 - Parse HTML in order to find the data file
        item_id_http = idutils.to_url(
            self.item_id,
            idutils.detect_identifier_schemes(self.item_id)[0],
            url_scheme="http",
        )
        resp = requests.head(item_id_http, allow_redirects=False, verify=False)
        if resp.status_code == 302:
            item_id_http = resp.headers["Location"]
        resp = requests.head(item_id_http + "?mode=full", verify=False)
        if resp.status_code == 200:
            item_id_http = item_id_http + "?mode=full"

        msg_2, points_2, data_files = self.find_dataset_file(
            self.metadata, item_id_http, self.supported_data_formats
        )
        if points_2 == 100 and points == 100:
            msg_list.append(
                {
                    "message": _("Data can be accessed manually") + " | %s" % msg_2,
                    "points": points_2,
                }
            )
        elif points_2 == 0 and points == 100:
            msg_list.append(
                {
                    "message": _("Data can not be accessed manually") + " | %s" % msg_2,
                    "points": points_2,
                }
            )
        elif points_2 == 100 and points == 0:
            msg_list.append(
                {
                    "message": _("Data can be accessed manually") + " | %s" % msg_2,
                    "points": points_2,
                }
            )
            points = 100
        elif points_2 == 0 and points == 0:
            msg_list.append(
                {
                    "message": _(
                        "No access information can be found in the metadata. Please, add information to the following term(s)"
                    )
                    + " %s" % term_data,
                    "points": points_2,
                }
            )

        return (points, msg_list)

    def rda_a1_03m(self, **kwargs):
        """Indicator RDA-A1-03M Metadata identifier resolves to a metadata record
        This indicator is linked to the following principle: A1: (Meta)data are retrievable by their
        identifier using a standardised communication protocol.
        This indicator is about the resolution of the metadata identifier. The identifier assigned to
        the metadata should be associated with a resolution service that enables access to the
        metadata record.
        Technical proposal:
        Parameters
        ----------
        item_id : str
            Digital Object identifier, which can be a generic one (DOI, PID), or an internal (e.g. an
            identifier from the repo)
        Returns
        -------
        points
            A number between 0 and 100 to indicate how well this indicator is supported
        msg
            Message with the results or recommendations to improve this indicator
        """
        # 1 - Look for the metadata terms in HTML in order to know if they can be accessed manueally
        points = 0
        msg_list = []
        try:
            item_id_http = idutils.to_url(
                self.item_id,
                idutils.detect_identifier_schemes(self.item_id)[0],
                url_scheme="http",
            )
            resp = requests.head(item_id_http, allow_redirects=False, verify=False)
            if resp.status_code == 302:
                item_id_http = resp.headers["Location"]
            resp = requests.head(item_id_http + "?mode=full", verify=False)
            if resp.status_code == 200:
                if "?mode=full" not in item_id_http:
                    item_id_http = item_id_http + "?mode=full"
            metadata_dc = self.metadata[
                self.metadata["metadata_schema"] == self.metadata_schemas["dc"]
            ]
            points, msg = ut.metadata_human_accessibility(metadata_dc, item_id_http)
            msg_list.append(
                {
                    "message": _("%s \nMetadata found via Identifier" % msg),
                    "points": points,
                }
            )
        except Exception as e:
            logger.error(e)
        try:
            points = (points * self.metadata_quality) / 100
            msg_list.append(
                {
                    "message": _("Total score after applying metadata quality factor")
                    + ": "
                    + points,
                    "points": points,
                }
            )
        except Exception as e:
            logger.error(e)
        if points == 0:
            msg_list.append(
                {"message": _("Metadata can not be found"), "points": points}
            )
        return (points, msg_list)

    def rda_a1_04m(self, return_protocol=False, **kwargs):
        """Indicator RDA-A1-04M: Metadata is accessed through standarised protocol.

        This indicator is linked to the following principle: A1: (Meta)data are retrievable by their
        identifier using a standardised communication protocol.

        The indicator concerns the protocol through which the metadata is accessed and requires
        the protocol to be defined in a standard.

        Returns
        -------
        points
            100/100 if the endpoint protocol is in the accepted list of standarised protocols
        msg
            Message with the results or recommendations to improve this indicator
        """
        points = 0

        protocol = ut.get_protocol_scheme(self.oai_base)
        if protocol in self.terms_access_protocols:
            points = 100
            msg = "Found a standarised protocol to access the metadata record: " + str(
                protocol
            )
        else:
            msg = (
                "Found a non-standarised protocol to access the metadata record: %s"
                % str(protocol)
            )
        msg_list = [{"message": msg, "points": points}]

        if return_protocol:
            return (points, msg_list, protocol)

        return (points, msg_list)

    def rda_a1_03d(self, **kwargs):
        """Indicator RDA-A1-01M.

        This indicator is linked to the following principle: A1: (Meta)data are retrievable by their
        identifier using a standardised communication protocol. More information about that
        principle can be found here.

        This indicator is about the resolution of the identifier that identifies the digital object. The
        identifier assigned to the data should be associated with a formally defined
        retrieval/resolution mechanism that enables access to the digital object, or provides access
        instructions for access in the case of human-mediated access. The FAIR principle and this
        indicator do not say anything about the mutability or immutability of the digital object that
        is identified by the data identifier -- this is an aspect that should be governed by a
        persistence policy of the data provider

        Returns
        -------
        points
            A number between 0 and 100 to indicate how well this indicator is supported
        msg
            Message with the results or recommendations to improve this indicator
        """
        msg_list = []
        points = 0
        expected_files = []

        try:
            # (1) Ensure file list is available in the object.
            if self.file_list is None or len(self.file_list) == 0:
                logger.debug("A1-03D: file_list not available. Resolving files from PID")
                resolved_files = self.resolve_item_files(self.item_id, self.id_type)
                self.file_list = pd.DataFrame(
                    resolved_files,
                    columns=["name", "extension", "format", "link", "final_url"],
                )
                msg_list.append(
                    {
                        "message": _(
                            "Resolved file list from PID. Files found"
                        )
                        + ": %s" % len(self.file_list),
                        "points": 0,
                    }
                )
            else:
                logger.debug(
                    "A1-03D: using existing file_list with %s entries",
                    len(self.file_list),
                )

            if "final_url" in self.file_list.columns:
                expected_files = self.file_list.to_dict("records")
            else:
                expected_files = self.file_list.to_dict("records")

            if len(expected_files) == 0:
                msg_list.append(
                    {
                        "message": _("No files available to evaluate manual download"),
                        "points": points,
                    }
                )
                return points, msg_list

            # (2) Resolve persistent identifier to landing page.
            id_scheme = idutils.detect_identifier_schemes(self.item_id)[0]
            pid_url = idutils.to_url(self.item_id, id_scheme, url_scheme="https")
            landing_response = requests.get(
                pid_url, allow_redirects=True, verify=False, timeout=20
            )
            landing_url = landing_response.url
            logger.debug("A1-03D: PID URL %s resolved to landing %s", pid_url, landing_url)
            msg_list.append(
                {
                    "message": _("PID resolves to landing page") + ": %s" % landing_url,
                    "points": 0,
                }
            )

            # (3) Human-like HTML check: file links must be present in landing page.
            soup = BeautifulSoup(landing_response.text, features="html.parser")
            html_links = set()
            html_texts = []
            for tag in soup.find_all("a"):
                href = tag.get("href")
                if href:
                    abs_link = urllib.parse.urljoin(landing_url, href)
                    html_links.add(abs_link)
                    html_links.add(urllib.parse.unquote(abs_link))
                txt = (tag.get_text() or "").strip()
                if txt:
                    html_texts.append(txt)

            logger.debug("A1-03D: extracted %s anchors from landing page", len(html_links))
            msg_list.append(
                {
                    "message": _("Links discovered in landing page HTML")
                    + ": %s" % len(html_links),
                    "points": 0,
                }
            )

            accessible_count = 0
            matched_count = 0
            for row in expected_files:
                name = row.get("name", "")
                link = row.get("link", "")
                final_url = row.get("final_url", "")

                candidates = set()
                if link:
                    candidates.add(link)
                    candidates.add(urllib.parse.unquote(link))
                if final_url:
                    candidates.add(final_url)
                    candidates.add(urllib.parse.unquote(final_url))

                # Match by URL in href or by visible text/file name.
                match_url = None
                for c in candidates:
                    if c in html_links:
                        match_url = c
                        break
                if match_url is None and name:
                    encoded_name = urllib.parse.quote(name)
                    for h in html_links:
                        if name in h or encoded_name in h:
                            match_url = h
                            break
                    if match_url is None:
                        for t in html_texts:
                            if name in t:
                                match_url = landing_url
                                break

                if match_url is None:
                    logger.debug("A1-03D: file not found in landing HTML: %s", name)
                    msg_list.append(
                        {
                            "message": _("File not present in landing page HTML")
                            + ": %s" % name,
                            "points": 0,
                        }
                    )
                    continue

                matched_count += 1
                logger.debug("A1-03D: file found in landing HTML: %s -> %s", name, match_url)
                msg_list.append(
                    {
                        "message": _("File present in landing page HTML")
                        + ": %s -> %s" % (name, match_url),
                        "points": 0,
                    }
                )

                # (4) Verify manual download URL accessibility.
                try:
                    check = requests.head(
                        match_url, allow_redirects=True, verify=False, timeout=20
                    )
                    if check.status_code == 405:
                        check = requests.get(
                            match_url,
                            allow_redirects=True,
                            verify=False,
                            timeout=20,
                            stream=True,
                        )
                    if check.status_code == 200:
                        accessible_count += 1
                        logger.debug(
                            "A1-03D: file is manually downloadable: %s (%s)",
                            name,
                            check.url,
                        )
                        msg_list.append(
                            {
                                "message": _("File is manually downloadable")
                                + ": %s" % name,
                                "points": 0,
                            }
                        )
                    else:
                        logger.debug(
                            "A1-03D: file is not downloadable (HTTP %s): %s",
                            check.status_code,
                            name,
                        )
                        msg_list.append(
                            {
                                "message": _("File is not manually downloadable")
                                + ": %s (HTTP %s)" % (name, check.status_code),
                                "points": 0,
                            }
                        )
                except Exception as e:
                    logger.error("A1-03D: error checking file download %s: %s", name, e)
                    msg_list.append(
                        {
                            "message": _("Error checking manual download")
                            + ": %s" % name,
                            "points": 0,
                        }
                    )

            total_files = len(expected_files)
            if total_files > 0:
                points = (accessible_count * 100) / total_files
            else:
                points = 0

            logger.debug(
                "A1-03D: summary total=%s matched_in_html=%s downloadable=%s points=%s",
                total_files,
                matched_count,
                accessible_count,
                points,
            )
            msg_list.append(
                {
                    "message": _("Manual download summary")
                    + ": total=%s, in_html=%s, downloadable=%s"
                    % (total_files, matched_count, accessible_count),
                    "points": points,
                }
            )

        except Exception as e:
            logger.error("A1-03D evaluation error: %s", e)
            msg_list.append(
                {"message": _("Error evaluating manual downloadability"), "points": 0}
            )

        return points, msg_list

    def rda_a1_05d(self, **kwargs):
        """Indicator RDA-A1-01M
        This indicator is linked to the following principle: A1: (Meta)data are retrievable by their
        identifier using a standardised communication protocol. More information about that
        principle can be found here.
        The indicator refers to automated interactions between machines to access digital objects.
        The way machines interact and grant access to the digital object will be evaluated by the
        indicator.
        Technical proposal:
        Parameters
        ----------
        item_id : str
            Digital Object identifier, which can be a generic one (DOI, PID), or an internal (e.g. an
            identifier from the repo)
        Returns
        -------
        points
            A number between 0 and 100 to indicate how well this indicator is supported
        msg
            Message with the results or recommendations to improve this indicator
        """
        msg_list = []
        points = 0
        if self.file_list is None:
            return super().rda_a1_05d()
        else:
            try:
                protocol = "http"
                number_of_files = len(self.file_list["link"])
                accessible_files = 0
                accessible_files_list = []
                for f in self.file_list["link"]:
                    try:
                        res = requests.head(f, verify=False, allow_redirects=True)
                        if res.status_code == 200:
                            accessible_files += 1
                            accessible_files_list.append(f)
                    except Exception as e:
                        logging.error(e)
                if accessible_files == number_of_files:
                    points = 100
                    msg_list.append(
                        {
                            "message": _("Data is accessible automatically via HTTP:")
                            + accessible_files_list,
                            "points": points,
                        }
                    )
                elif accessible_files == 0:
                    points = 0
                    msg_list.append(
                        {
                            "message": _("Files are not accessible via HTTP"),
                            "points": points,
                        }
                    )
                else:
                    points = (accessible_files * 100) / number_of_files
                    msg_list.append(
                        {
                            "message": _(
                                "Some of digital objects are accessible automatically via HTTP:"
                            )
                            + accessible_files_list,
                            "points": points,
                        }
                    )
            except Exception as e:
                logging.debug(e)

        return points, msg_list

    def rda_a1_2_01d(self, **kwargs):
        """Indicator RDA-A1-01M
        This indicator is linked to the following principle: A1.2: The protocol allows for an
        authentication and authorisation where necessary. More information about that principle
        can be found here.
        The indicator requires the way that access to the digital object can be authenticated and
        authorised and that data accessibility is specifically described and adequately documented.
        Technical proposal:
        Parameters
        ----------
        item_id : str
            Digital Object identifier, which can be a generic one (DOI, PID), or an internal (e.g. an
            identifier from the repo)
        Returns
        -------
        points
            A number between 0 and 100 to indicate how well this indicator is supported
        msg
            Message with the results or recommendations to improve this indicator
        """
        points = 100
        msg = _(
            "DIGITAL.CSIC allow access management and authentication and authorisation from CSIC CAS"
        )

        return points, [{"message": msg, "points": points}]

    def rda_a2_01m(self, **kwargs):
        """Indicator RDA-A1-01M
        This indicator is linked to the following principle: A2: Metadata should be accessible even
        when the data is no longer available. More information about that principle can be found
        here.
        The indicator intends to verify that information about a digital object is still available after
        the object has been deleted or otherwise has been lost. If possible, the metadata that
        remains available should also indicate why the object is no longer available.
        Technical proposal:
        Parameters
        ----------
        item_id : str
            Digital Object identifier, which can be a generic one (DOI, PID), or an internal (e.g. an
            identifier from the repo)
        Returns
        -------
        points
            A number between 0 and 100 to indicate how well this indicator is supported
        msg
            Message with the results or recommendations to improve this indicator
        """
        points = 100
        msg = _(
            "DIGITAL.CSIC preservation policy is available at: https://digital.csic.es/dc/politicas/#politica8"
        )
        return points, [{"message": msg, "points": points}]

        # INTEROPERABLE

    def rda_i1_01d(self, **kwargs):
        """Indicator RDA-A1-01M
        This indicator is linked to the following principle: I1: (Meta)data use a formal, accessible,
        shared, and broadly applicable language for knowledge representation. More information
        about that principle can be found here.

        The indicator serves to determine that an appropriate standard is used to express
        knowledge, in particular the data model and format.
        Technical proposal: Data format is within a list of accepted standards.


        Returns
        -------
        points
            A number between 0 and 100 to indicate how well this indicator is supported
        msg
            Message with the results or recommendations to improve this indicator
        """
        points = 0
        msg_list = []
        msg = "No internet media file path found"
        internetMediaFormats = []
        availableFormats = []
        path = self.internet_media_types_path[0]
        supported_data_formats = [
            ".tif",
            ".aig",
            ".asc",
            ".agr",
            ".grd",
            ".nc",
            ".hdf",
            ".hdf5",
            ".pdf",
            ".odf",
            ".doc",
            ".docx",
            ".csv",
            ".jpg",
            ".png",
            ".gif",
            ".mp4",
            ".xml",
            ".rdf",
            ".txt",
            ".mp3",
            ".wav",
            ".zip",
            ".rar",
            ".tar",
            ".tar.gz",
            ".jpeg",
            ".xls",
            ".xlsx",
        ]

        try:
            f = open(path)
            f.close()

        except:
            msg = "The config.ini internet media types file path does not arrive at any file. Try 'static/internetmediatipes190224.csv'"
            logger.error(msg)
            return (points, [{"message": msg, "points": points}])
        logger.debug("Trying to open accepted media formats")
        f = open(path)
        csv_reader = csv.reader(f)

        for row in csv_reader:
            internetMediaFormats.append(row[1])

        f.close()
        for e in supported_data_formats:
            internetMediaFormats.append(e)
        logger.debug("List: %s" % internetMediaFormats)

        try:
            item_id_http = idutils.to_url(
                self.item_id,
                idutils.detect_identifier_schemes(self.item_id)[0],
                url_scheme="http",
            )
            logger.debug("Searching for dataset files")
            points, msg, data_files = self.find_dataset_file(
                self.item_id, item_id_http, internetMediaFormats
            )
            for e in data_files:
                logger.debug(e)
            msg_list.append({"message": msg, "points": points})
            if points == 0:
                msg_list.append({"message": _("No files found"), "points": points})
        except Exception as e:
            logger.error(e)

        return (points, msg_list)

    def rda_i1_02m(self, **kwargs):
        """Indicator RDA-A1-01M
        This indicator is linked to the following principle: I1: (Meta)data use a formal, accessible,
        shared, and broadly applicable language for knowledge representation. More information
        about that principle can be found here.
        This indicator focuses on the machine-understandability aspect of the metadata. This means
        that metadata should be readable and thus interoperable for machines without any
        requirements such as specific translators or mappings.
        Technical proposal:
        Parameters
        ----------
        item_id : str
            Digital Object identifier, which can be a generic one (DOI, PID), or an internal (e.g. an
            identifier from the repo)
        Returns
        -------
        points
            A number between 0 and 100 to indicate how well this indicator is supported
        msg
            Message with the results or recommendations to improve this indicator
        """
        identifier_temp = self.item_id
        df = pd.DataFrame(self.metadata)

        # Hacer la selección donde la columna 'term' es igual a 'identifier' y 'qualifier' es igual a 'uri'
        selected_handle = df.loc[
            (df["element"] == "identifier") & (df["qualifier"] == "uri"), "text_value"
        ]
        self.item_id = ut.get_handle_str(selected_handle.iloc[0])
        points, msg_list = super().rda_i1_02m()
        try:
            points = (points * self.metadata_quality) / 100
            msg_list.append({"message": _("After applying weigh"), "points": points})
        except Exception as e:
            logging.error(e)
        self.item_id = identifier_temp
        return (points, msg_list)

    @ConfigTerms(term_id="terms_qualified_references")
    def rda_i3_01m(self, **kwargs):
        """Indicator RDA-A1-01M
        This indicator is linked to the following principle: I3: (Meta)data include qualified references
        to other (meta)data. More information about that principle can be found here.
        The indicator is about the way that metadata is connected to other metadata, for example
        through links to information about organisations, people, places, projects or time periods
        that are related to the digital object that the metadata describes.
        Technical proposal:
        Parameters
        ----------
        item_id : str
            Digital Object identifier, which can be a generic one (DOI, PID), or an internal (e.g. an
            identifier from the repo)
        Returns
        -------
        points
            A number between 0 and 100 to indicate how well this indicator is supported
        msg
            Message with the results or recommendations to improve this indicator
        """
        points = 0
        msg_list = []

        term_data = self._get_config_term_from_kwargs(
            kwargs, "terms_qualified_references"
        )
        term_metadata = term_data["metadata"]
        try:
            for index, row in term_metadata.iterrows():
                if ut.check_standard_project_relation(row["text_value"]):
                    points = 100
                    msg_list.append(
                        {
                            "message": _("Qualified references to related object")
                            + ": "
                            + row["text_value"],
                            "points": points,
                        }
                    )
                elif ut.check_controlled_vocabulary(row["text_value"]):
                    points = 100
                    msg_list.append(
                        {
                            "message": _("Qualified references to related object")
                            + ": "
                            + row["text_value"],
                            "points": points,
                        }
                    )
        except Exception as e:
            logging.error("Error in I3_01M: %s" % e)
        return (points, msg_list)

    @ConfigTerms(term_id="terms_relations")
    def rda_i3_02m(self, **kwargs):
        """Indicator RDA-I3-02M
        This indicator is linked to the following principle: I3: (Meta)data include qualified references
        to other (meta)data. More information about that principle can be found here.
        This indicator is about the way metadata is connected to other data, for example linking to
        previous or related research data that provides additional context to the data. Please note
        that this is not about the link from the metadata to the data it describes; that link is
        considered in principle F3 and in indicator RDA-F3-01M.
        Technical proposal:
        Parameters
        ----------
        item_id : str
            Digital Object identifier, which can be a generic one (DOI, PID), or an internal (e.g. an
            identifier from the repo)
        Returns
        -------
        points
            A number between 0 and 100 to indicate how well this indicator is supported
        msg
            Message with the results or recommendations to improve this indicator
        """
        points = 0
        msg_list = []

        term_data = self._get_config_term_from_kwargs(kwargs, "terms_relations")
        term_metadata = term_data["metadata"]
        try:
            for index, row in term_metadata.iterrows():
                if ut.check_standard_project_relation(row["text_value"]):
                    points = 100
                    msg_list.append(
                        {
                            "message": _("References to related object")
                            + ": "
                            + row["text_value"],
                            "points": points,
                        }
                    )
                elif ut.check_controlled_vocabulary(row["text_value"]):
                    points = 100
                    msg_list.append(
                        {
                            "message": _("References to related object")
                            + ": "
                            + row["text_value"],
                            "points": points,
                        }
                    )
                elif ut.get_orcid_str(row["text_value"]) != "":
                    if ut.check_orcid(row["text_value"]):
                        points = 100
                        msg_list.append(
                            {
                                "message": _("References to ORCID")
                                + ": "
                                + row["text_value"],
                                "points": points,
                            }
                        )

        except Exception as e:
            logger.error("Error in I3_02M: %s" % e)
        return (points, msg_list)

    @ConfigTerms(term_id="terms_relations")
    def rda_i3_02d(self, **kwargs):
        """Indicator RDA-A1-01M
        This indicator is linked to the following principle: I3: (Meta)data include qualified references
        to other (meta)data. More information about that principle can be found here.
        Description of the indicator RDA-I3-02D
        This indicator is about the way data is connected to other data. The references need to be
        qualified which means that the relationship role of the related resource is specified, for
        example that a particular link is a specification of a unit of m
        Technical proposal:
        Parameters
        ----------
        item_id : str
            Digital Object identifier, which can be a generic one (DOI, PID), or an internal (e.g. an
            identifier from the repo)
        Returns
        -------
        points
            A number between 0 and 100 to indicate how well this indicator is supported
        msg
            Message with the results or recommendations to improve this indicator
        """
        return self.rda_i3_02m(**kwargs)

    @ConfigTerms(term_id="terms_relations")
    def rda_i3_03m(self, **kwargs):
        """Indicator RDA-I3-03M
        This indicator is linked to the following principle: I3: (Meta)data include qualified references
        to other (meta)data. More information about that principle can be found here.
        This indicator is about the way metadata is connected to other data, for example linking to
        previous or related research data that provides additional context to the data. Please note
        that this is not about the link from the metadata to the data it describes; that link is
        considered in principle F3 and in indicator RDA-F3-01M.
        Technical proposal:
        Parameters
        ----------
        item_id : str
            Digital Object identifier, which can be a generic one (DOI, PID), or an internal (e.g. an
            identifier from the repo)
        Returns
        -------
        points
            A number between 0 and 100 to indicate how well this indicator is supported
        msg
            Message with the results or recommendations to improve this indicator
        """
        return self.rda_i3_02m(**kwargs)

    def rda_r1_1_03m(self, machine_readable=True, **kwargs):
        """Indicator R1.1-03M: Metadata refers to a machine-understandable reuse
        license.

        This indicator is linked to the following principle: R1.1: (Meta)data are released with a clear
        and accessible data usage license.

        This indicator is about the way that the reuse licence is expressed. Rather than being a human-readable text, the licence should be expressed in such a way that it can be processed by machines, without human intervention, for example in automated searches.

        Returns
        -------
        points
            100/100 if the license is provided in such a way that is machine understandable
        msg
            Message with the results or recommendations to improve this indicator
        """
        return super().rda_r1_1_03m(machine_readable=False)

    @ConfigTerms(term_id="prov_terms")
    def rda_r1_2_01m(self, **kwargs):
        """Indicator RDA-A1-01M
        This indicator is linked to the following principle: R1.2: (Meta)data are associated with
        detailed provenance. More information about that principle can be found here.
        This indicator requires the metadata to include information about the provenance of the
        data, i.e. information about the origin, history or workflow that generated the data, in a
        way that is compliant with the standards that are used in the community in which the data
        is produced.
        Technical proposal:
        Parameters
        ----------
        item_id : str
            Digital Object identifier, which can be a generic one (DOI, PID), or an internal (e.g. an
            identifier from the repo)
        Returns
        -------
        points
            A number between 0 and 100 to indicate how well this indicator is supported
        msg
            Message with the results or recommendations to improve this indicator
        """
        points = 0
        msg_list = []

        term_data = self._get_config_term_from_kwargs(kwargs, "prov_terms")
        logger.debug(term_data)
        term_metadata = term_data["metadata"]
        logger.debug(term_metadata.element)
        id_list = []
        try:
            for index, row in term_metadata.iterrows():
                _points = 100
                msg_list.append(
                    {
                        "message": _("Provenance info found")
                        + ": %s" % (row["text_value"]),
                        "points": _points,
                    }
                )
            points = (
                100 * len(term_metadata[["element", "qualifier"]].drop_duplicates())
            ) / len(term_data["list"])

        except Exception as e:
            logger.error("Error in I3_02M: %s" % e)

        if points == 0:
            msg_list.append(
                {
                    "message": _(
                        "Provenance information can not be found. Please, include the info in config.ini"
                    ),
                    "points": points,
                }
            )
        return (points, msg_list)

    def rda_r1_3_01m(self, **kwargs):
        """Indicator RDA-A1-01M
        This indicator is linked to the following principle: R1.3: (Meta)data meet domain-relevant
        community standards.
        This indicator requires that metadata complies with community standards.
        Technical proposal:
        Parameters
        ----------
        item_id : str
            Digital Object identifier, which can be a generic one (DOI, PID), or an internal (e.g. an
            identifier from the repo)
        Returns
        -------
        points
            A number between 0 and 100 to indicate how well this indicator is supported
        msg
            Message with the results or recommendations to improve this indicator
        """

        points = 0
        msg_list = []

        try:
            for e in self.metadata.metadata_schema.unique():
                logger.debug("Checking: %s" % e)
                logger.debug("Trying: %s" % self.metadata_schemas["dc"])
                if e == self.metadata_schemas["dc"]:  # Check Dublin Core
                    if ut.check_url(e):
                        points = 100
                        msg_list.append(
                            {
                                "message": _(
                                    "DIGITAL.CSIC supports qualified Dublin Core as well as other discipline agnostics schemes like DataCite. Terms found"
                                ),
                                "points": points,
                            }
                        )
        except Exception as e:
            logger.error("Problem loading plugin config: %s" % e)
        try:
            points = (points * self.metadata_quality) / 100
        except Exception as e:
            logger.error(e)
        if points == 0:
            msg_list.append(
                {
                    "message": _(
                        "Currently, this repo does not include community-bsed schemas. If you need to include yours, please contact."
                    ),
                    "points": points,
                }
            )

        return (points, msg_list)

    def rda_r1_3_01d(self, **kwargs):
        """Indicator RDA-R1.3-01D: Data complies with a community standard.

        This indicator is linked to the following principle: R1.3: (Meta)data meet domain-relevant
        community standards.

        This indicator requires that data complies with community standards.

        Returns
        --------
        points
           100/100 if the data standard appears in Fairsharing (0/100 otherwise)
        """
        return self.rda_i1_01d()

    def rda_r1_3_02m(self, **kwargs):
        """Indicator RDA-A1-01M
        This indicator is linked to the following principle: R1.3: (Meta)data meet domain-relevant
        community standards. More information about that principle can be found here.
        This indicator requires that the metadata follows a community standard that has a machineunderstandable expression.
        Technical proposal:
        Parameters
        ----------
        item_id : str
            Digital Object identifier, which can be a generic one (DOI, PID), or an internal (e.g. an
            identifier from the repo)
        Returns
        -------
        points
            A number between 0 and 100 to indicate how well this indicator is supported
        msg
            Message with the results or recommendations to improve this indicator
        """
        points, msg_list = super().rda_r1_3_02m()
        try:
            points = (points * self.metadata_quality) / 100
            msg_list.append(
                {
                    "message": _("Total score after applying metadata quality factor")
                    + ": %f" % points,
                    "points": points,
                }
            )
        except Exception as e:
            logger.error(e)

        return (points, msg_list)

    # DIGITAL_CSIC UTILS
    def get_internal_id(self, item_id, connection):
        internal_id = item_id
        id_to_check = ut.get_doi_str(item_id)
        logger.debug("DOI is %s" % id_to_check)
        temp_str = "%" + item_id + "%"
        if len(id_to_check) != 0:
            if ut.check_doi(id_to_check):
                query = (
                    "SELECT item.item_id FROM item, metadatavalue, metadatafieldregistry WHERE item.item_id = metadatavalue.resource_id AND metadatavalue.metadata_field_id = metadatafieldregistry.metadata_field_id AND metadatafieldregistry.element = 'identifier' AND metadatavalue.text_value LIKE '%s'"
                    % temp_str
                )
                logger.debug(query)
                cursor = connection.cursor()
                cursor.execute(query)
                list_id = cursor.fetchall()
                if len(list_id) > 0:
                    for row in list_id:
                        internal_id = row[0]

        if internal_id == item_id:
            id_to_check = ut.get_handle_str(item_id)
            logger.debug("PID is %s" % id_to_check)
            temp_str = "%" + item_id + "%"
            query = (
                "SELECT item.item_id FROM item, metadatavalue, metadatafieldregistry WHERE item.item_id = metadatavalue.resource_id AND metadatavalue.metadata_field_id = metadatafieldregistry.metadata_field_id AND metadatafieldregistry.element = 'identifier' AND metadatavalue.text_value LIKE '%s'"
                % temp_str
            )
            logger.debug(query)
            cursor = connection.cursor()
            cursor.execute(query)
            list_id = cursor.fetchall()
            if len(list_id) > 0:
                for row in list_id:
                    internal_id = row[0]

        return internal_id

    def get_handle_id(self, internal_id, connection):
        query = (
            "SELECT metadatavalue.text_value FROM item, metadatavalue, metadatafieldregistry WHERE item.item_id = %s AND item.item_id = metadatavalue.resource_id AND metadatavalue.metadata_field_id = metadatafieldregistry.metadata_field_id AND metadatafieldregistry.element = 'identifier' AND metadatafieldregistry.qualifier = 'uri'"
            % internal_id
        )
        cursor = connection.cursor()
        cursor.execute(query)
        list_id = cursor.fetchall()
        handle_id = ""
        if len(list_id) > 0:
            for row in list_id:
                handle_id = row[0]

        return ut.get_handle_str(handle_id)

    def _get_config_term_from_kwargs(self, kwargs, term_id):
        """Normalize ConfigTerms payload from kwargs to avoid KeyError."""
        empty_md = pd.DataFrame(
            columns=["text_value", "metadata_schema", "element", "qualifier"]
        )
        term_data = kwargs.get(term_id, {}) if isinstance(kwargs, dict) else {}
        if not isinstance(term_data, dict):
            term_data = {}
        term_metadata = term_data.get("metadata", empty_md)
        if term_metadata is None:
            term_metadata = empty_md
        term_list = term_data.get("list", [])
        if term_list is None:
            term_list = []
        return {"metadata": term_metadata, "list": term_list}

    def metadata_prefix_to_uri(self, prefix):
        uri = prefix
        try:
            logging.debug("TEST A102M: we have this prefix: %s" % prefix)
            metadata_schemas = ast.literal_eval(
                self.config[self.name]["metadata_schemas"]
            )
            if prefix in metadata_schemas:
                uri = metadata_schemas[prefix]
        except Exception as e:
            logger.error("TEST A102M: Problem loading plugin config: %s" % e)
        return uri

    def find_dataset_file(self, metadata, url, data_formats):
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
        }
        response = requests.get(url, headers=headers, verify=False)
        soup = BeautifulSoup(response.text, features="html.parser")

        msg = "No dataset files found"
        points = 0

        data_files = []
        for tag in soup.find_all("a"):
            for f in data_formats:
                try:
                    if f in tag.get("href") or f in tag.text:
                        data_files.append(tag.get("href"))
                except Exception as e:
                    pass

        if len(data_files) > 0:
            self.data_files = data_files
            points = 100
            msg = "Potential datasets files found: %s" % data_files

        return points, msg, data_files
