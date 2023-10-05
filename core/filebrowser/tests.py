import os

from django.conf import settings
import unittest
from django.test.client import Client

from .tests_data import TESTS_DATA
from .utils import get_filebrowser_vo, get_filebrowser_directory, \
get_fullpath_filebrowser_directory, get_rucio_account, get_x509_proxy, \
get_capath, get_rucio_redirect_host, get_rucio_rest_api_auth_host, \
get_rucio_rest_api_server_host


class SimpleFileBrowserTest(unittest.TestCase):
    def setUp(self):
        # Every test needs a client.
        self.client = Client()

    # @unittest2.skip('skipping on purpose')
    def test_settings_vo(self):
        """
            test_settings_vo
            
            Test that FILEBROWSER_VO is defined in settings, 
            and that its value is retrieved correctly in get_filebrowser_vo
            
        """
        vo = get_filebrowser_vo()
        self.assertEqual(vo, getattr(settings, "FILEBROWSER_VO", "atlas"))


    # @unittest2.skip('skipping on purpose')
    def test_settings_filebrowser_directory(self):
        """
            test_settings_filebrowser_directory

            Test that FILEBROWSER_DIRECTORY is defined in settings, 
            and that its value is retrieved correctly in get_filebrowser_directory
        """
        dir = get_filebrowser_directory()
        self.assertEqual(dir, getattr(settings, "FILEBROWSER_DIRECTORY", "filebrowser"))


    @unittest.skip('skipping on purpose')
    def test_settings_fullpath_filebrowser_directory(self):
        """
            test_settings_fullpath_filebrowser_directory

            Test that MEDIA_ROOT+'/'+FILEBROWSER_DIRECTORY directory exists on the filesystem, 
            and that its value is retrieved correctly in get_fullpath_filebrowser_directory
        """
        full_dir = get_fullpath_filebrowser_directory()
        ### test that full_dirr is correctly assembled by get_fullpath_filebrowser_directory
        self.assertEqual(full_dir, \
                    getattr(settings, "MEDIA_ROOT", "/tmp") + '/' \
                    + getattr(settings, "FILEBROWSER_DIRECTORY", "filebrowser"))
        ### test that full_dir exists
        self.assertEqual(os.path.isdir(full_dir), True)


    @unittest.skip('skipping on purpose')
    def test_settings_x509_proxy(self):
        """
            test_settings_x509_proxy

            Test that X509_USER_PROXY is defined in settings 
            and that its value is retrieved correctly in get_x509_proxy
        """
        x509_user_proxy = get_x509_proxy()
        self.assertEqual(x509_user_proxy, \
                    getattr(settings, "X509_USER_PROXY", \
                            "/data/atlpan/x509up_u25606"))


    @unittest.skip('skipping on purpose')
    def test_settings_capath(self):
        """
            test_settings_capath

            Test that CAPATH is defined in settings 
            and that its value is retrieved correctly in get_capath
        """
        capath = get_capath()
        self.assertEqual(capath, getattr(settings, "CAPATH", "/etc/grid-security/certificates"))


    # @unittest2.skip('skipping on purpose')
    def test_settings_rucio_account(self):
        """
            test_settings_rucio_account

            Test that RUCIO_ACCOUNT is defined in settings 
            and that its value is retrieved correctly in get_rucio_account
        """
        rucio_account = get_rucio_account()
        self.assertEqual(rucio_account, getattr(settings, "RUCIO_ACCOUNT", "atlpan"))


    @unittest.skip('skipping on purpose')
    def test_settings_rucio_hosts(self):
        """
            test_settings_rucio_hosts

            Test that RUCIO_REDIRECT_HOST, RUCIO_AUTH_HOST. RUCIO_SERVER_HOST
            are defined in settings 
            and that their value is retrieved correctly in get_rucio_redirect_host, 
            get_rucio_rest_api_auth_host, get_rucio_rest_api_server_host
        """
        ### redirect host
        host_redirect = get_rucio_redirect_host()
        self.assertEqual(host_redirect, getattr(settings, "RUCIO_REDIRECT_HOST", "https://rucio-lb-prod.cern.ch"))
        ### auth host
        host_auth = get_rucio_rest_api_auth_host()
        self.assertEqual(host_auth, getattr(settings, "RUCIO_AUTH_HOST", "https://voatlasrucio-auth-prod.cern.ch"))
        ### server host
        host_server = get_rucio_rest_api_server_host()
        self.assertEqual(host_server, getattr(settings, "RUCIO_SERVER_HOST", "https://voatlasrucio-server-prod.cern.ch"))



