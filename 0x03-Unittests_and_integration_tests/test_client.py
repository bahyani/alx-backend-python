#!/usr/bin/env python3
"""Unit tests for client module."""

import unittest
from parameterized import parameterized
from unittest.mock import patch, Mock, PropertyMock
from client import GithubOrgClient


class TestGithubOrgClient(unittest.TestCase):
    """Test class for GithubOrgClient."""

    @parameterized.expand([
        ("google",),
        ("abc",)
    ])
    @patch('client.get_json', return_value={"mocked": True})
    def test_org(self, org_name, mock_get_json):
        """Test that GithubOrgClient.org returns correct value and calls get_json once."""
        client = GithubOrgClient(org_name)
        result = client.org
        mock_get_json.assert_called_once_with(f"https://api.github.com/orgs/{org_name}")
        self.assertEqual(result, {"mocked": True})

    def test_public_repos_url(self):
        """Test GithubOrgClient._public_repos_url property returns correct URL from mocked org."""
        client = GithubOrgClient("test_org")
        payload = {"repos_url": "https://api.github.com/orgs/test_org/repos"}

        # Patch the org property of client to return the mocked payload
        with patch.object(GithubOrgClient, "org", new_callable=PropertyMock) as mock_org:
            mock_org.return_value = payload
            result = client._public_repos_url
            self.assertEqual(result, payload["repos_url"])


if __name__ == '__main__':
    unittest.main()
