# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from datetime import timedelta
from unittest.mock import patch

import pytest
from requests import HTTPError

from azure.kusto.data import ClientRequestProperties, KustoClient
from azure.kusto.data.exceptions import KustoServiceError
from tests.kusto_client_common import KustoClientTestsMixin, mocked_requests_post


class FakeResponse:
    """A response whose body is not Kusto JSON, like the page a proxy or a redirect returns."""

    reason = ""
    headers = None

    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text
        self.url = "https://somecluster.kusto.windows.net"

    def json(self):
        raise ValueError("not json")

    def raise_for_status(self):
        if self.status_code >= 400:
            raise HTTPError("{} Error".format(self.status_code), response=self)


class TestClientRequests(KustoClientTestsMixin):
    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_streaming_query_uses_the_default_database(self, mock_post):
        with KustoClient(self.HOST) as client:
            client.execute_streaming_query(None, "Deft")
        assert mock_post.call_args.kwargs["json"]["db"] == "NetDefaultDB"

    @pytest.mark.parametrize("method", [KustoClient.execute_query, KustoClient.execute_streaming_query])
    @patch("requests.Session.post")
    def test_redirect_says_so(self, mock_post, method):
        mock_post.return_value = FakeResponse(302)
        with KustoClient(self.HOST) as client:
            with pytest.raises(KustoServiceError, match="Unexpected redirection, got status code: 302"):
                method(client, "PythonTest", "Deft")

    @patch("requests.Session.post")
    def test_streaming_query_error_without_json(self, mock_post):
        mock_post.return_value = FakeResponse(502, "<html>Bad Gateway</html>")
        with KustoClient(self.HOST) as client:
            with pytest.raises(KustoServiceError, match="Bad Gateway"):
                client.execute_streaming_query("PythonTest", "Deft")

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_timeout_of_a_day_or_more(self, mock_post):
        properties = ClientRequestProperties()
        properties.set_option(ClientRequestProperties.request_timeout_option_name, timedelta(days=1))
        with KustoClient(self.HOST) as client:
            client.execute_query("PythonTest", "Deft", properties)
        # The timeout of the request plus the 30 seconds the client allows the service on top of it
        assert mock_post.call_args.kwargs["timeout"] == timedelta(days=1, seconds=30).total_seconds()
