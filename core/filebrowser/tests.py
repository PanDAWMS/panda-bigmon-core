#import commands
import datetime
import os

#from django.utils import timezone
from django.conf import settings
from django.utils import unittest
from django.test.client import Client

from .tests_data import TESTS_DATA
from .utils import get_filebrowser_vo, get_filebrowser_directory, \
get_fullpath_filebrowser_directory, get_rucio_account, get_x509_proxy, \
get_capath, get_rucio_redirect_host, get_rucio_rest_api_auth_host, \
get_rucio_rest_api_server_host, get_rucio_oauth_token, \
get_rucio_metalink_file, get_surls_from_rucio_metalink_file, \
get_rucio_pfns_from_guids_with_rucio_metalink_file, fetch_file, \
get_rucio_redirect_url, get_location_from_rucio_redirect_output, \
get_rucio_redirect_response, get_rucio_pfns_from_guids_with_rucio_redirect, \
get_rucio_pfns_from_guids_with_dq2client, \
get_filebrowser_hostname



class SimpleFileBrowserTest(unittest.TestCase):
    def setUp(self):
        # Every test needs a client.
        self.client = Client()

    @unittest.skip('skipping on purpose')
    def test_settings_vo(self):
        """
            test_settings_vo
            
            Test that FILEBROWSER_VO is defined in settings, 
            and that its value is retrieved correctly in get_filebrowser_vo
            
        """
        vo = get_filebrowser_vo()
        self.assertEqual(vo, getattr(settings, "FILEBROWSER_VO", "atlas"))


    @unittest.skip('skipping on purpose')
    def test_settings_hostname(self):
        """
            test_settings_hostname
            
            Test that FILEBROWSER_HOSTNAME is defined in settings, 
            and that its value is retrieved correctly in get_filebrowser_hostname
            
        """
        hostname = get_filebrowser_hostname()
        self.assertEqual(hostname, getattr(settings, "FILEBROWSER_HOSTNAME", \
                                           commands.getoutput('hostname')))


    @unittest.skip('skipping on purpose')
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
        self.assertEqual(capath, \
                    getattr(settings, "CAPATH", \
                            "/etc/grid-security/certificates"))


    @unittest.skip('skipping on purpose')
    def test_settings_rucio_account(self):
        """
            test_settings_rucio_account

            Test that RUCIO_ACCOUNT is defined in settings 
            and that its value is retrieved correctly in get_rucio_account
        """
        rucio_account = get_rucio_account()
        self.assertEqual(rucio_account, \
                    getattr(settings, "RUCIO_ACCOUNT", "atlpan"))


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
        self.assertEqual(host_redirect, \
                    getattr(settings, "RUCIO_REDIRECT_HOST", \
                            "https://rucio-lb-prod.cern.ch"))
        ### auth host
        host_auth = get_rucio_rest_api_auth_host()
        self.assertEqual(host_auth, \
                    getattr(settings, "RUCIO_AUTH_HOST", \
                            "https://voatlasrucio-auth-prod.cern.ch"))
        ### server host
        host_server = get_rucio_rest_api_server_host()
        self.assertEqual(host_server, \
                    getattr(settings, "RUCIO_SERVER_HOST", \
                            "https://voatlasrucio-server-prod.cern.ch"))


    def test_download_with_rucio_metalink_file(self, \
                test_file_config='test_file_exists', \
                eval_len_metalink=True, eval_len_surls=True, \
                eval_len_errors_pfn=False, \
                eval_len_errors=False, eval_len_pfns=True, \
                eval_len_files=True, eval_len_dirprefix=True, \
                dirpath_exists=True, dirpath_listing=True, \
                fpath_exists=True):
        """
            test_download_with_rucio_metalink_file

            1. Obtain Rucio OAuth Token.
            2. Get the metalink file
            3. Get surls from the metalink file
            4. Get PFNs from guid,site,lfn,scope
            5. Fetch file with PFN
            6. Test that log directory and files exist and have expected sizes
        """
        ### test file properties
        lfn = TESTS_DATA[test_file_config]['lfn']
        scope = TESTS_DATA[test_file_config]['scope']
        guid = TESTS_DATA[test_file_config]['guid']
        site = TESTS_DATA[test_file_config]['site']
        ### rucio oauth token
        oauth_token = get_rucio_oauth_token()
        self.assertEqual(len(oauth_token) > 0, True)
        ### metalink file
        metalink = get_rucio_metalink_file(oauth_token, lfn, scope)
        self.assertEqual(len(metalink) > 0, eval_len_metalink)
        ### surls
        surls = get_surls_from_rucio_metalink_file(metalink)
        self.assertEqual(len(surls) > 0, eval_len_surls)
        ### pfns
        pfns, errors = get_rucio_pfns_from_guids_with_rucio_metalink_file(\
                        [guid, ], site, [lfn, ], [scope, ])
        self.assertEqual(len(errors) > 0, eval_len_errors_pfn)
        self.assertEqual(len(pfns) > 0, eval_len_pfns)
        ### fetch file with PFN
        if len(pfns)>0:
            pfn = pfns[0]
        else:
            pfn='na'
        files, errtxt, dirprefix = fetch_file(pfn, guid)
        self.assertEqual(len(errors) > 0, eval_len_errors)
        self.assertEqual(len(files) > 0, eval_len_files)
        self.assertEqual(len(dirprefix) > 0, eval_len_dirprefix)
        ### test that the log tarball directory exists
        dirpath = getattr(settings, "MEDIA_ROOT", "/tmp") + '/' + dirprefix
        self.assertEqual(os.path.isdir(dirpath), dirpath_exists)
        self.assertEqual(len(os.listdir(dirpath)) > 0, dirpath_listing)
        ### test that the files exist and have expected size
        for f in files:
            if 'name' in f and 'size' in f:
                f_name = f['name']
                f_size = f['size']
                f_path = os.path.join(dirpath, f_name)
                self.assertEqual(os.path.exists(f_path), fpath_exists)
                self.assertEqual(os.path.getsize(f_path), f_size)


    def test_download_with_rucio_redirect(self, \
                test_file_config='test_file_exists', \
                eval_len_redirectURL=True, eval_len_surls=True, \
                eval_len_errors_pfn=False, \
                eval_len_errors=False, eval_len_pfns=True, \
                eval_len_files=True, eval_len_dirprefix=True, \
                dirpath_exists=True, dirpath_listing=True, \
                fpath_exists=True):
        """
            test_download_with_rucio_redirect

            1. Obtain Rucio OAuth Token.
            2. Get the Rucio redirect URL
            3. Get the Rucio redirect response
            4. Get PFNs from guid,site,lfn,scope
            5. Fetch file with PFN
            6. Test that log directory and files exist and have expected sizes
        """
        ### test file properties
        lfn = TESTS_DATA[test_file_config]['lfn']
        scope = TESTS_DATA[test_file_config]['scope']
        guid = TESTS_DATA[test_file_config]['guid']
        site = TESTS_DATA[test_file_config]['site']
        ### rucio oauth token
        oauth_token = get_rucio_oauth_token()
        self.assertEqual(len(oauth_token) > 0, True)
        ### redirect URL
        redirect_URL = get_rucio_redirect_url(lfn, scope)
        self.assertEqual(len(redirect_URL) > 0, eval_len_redirectURL)
        ### surl
        surl = get_rucio_redirect_response(redirect_URL)
        self.assertEqual(len(surl) > 0, eval_len_surls)
        ### pfns
        pfns, errors = get_rucio_pfns_from_guids_with_rucio_redirect(\
                        [guid, ], site, [lfn, ], [scope, ])
        self.assertEqual(len(errors) > 0, eval_len_errors_pfn)
        self.assertEqual(len(pfns) > 0, eval_len_pfns)
        ### fetch file with PFN
        if len(pfns)>0:
            pfn = pfns[0]
        else:
            pfn='na'
        files, errtxt, dirprefix = fetch_file(pfn, guid)
        self.assertEqual(len(errors) > 0, eval_len_errors)
        self.assertEqual(len(files) > 0, eval_len_files)
        self.assertEqual(len(dirprefix) > 0, eval_len_dirprefix)
        ### test that the log tarball directory exists
        dirpath = getattr(settings, "MEDIA_ROOT", "/tmp") + '/' + dirprefix
        self.assertEqual(os.path.isdir(dirpath), dirpath_exists)
        self.assertEqual(len(os.listdir(dirpath)) > 0, dirpath_listing)
        ### test that the files exist and have expected size
        for f in files:
            if 'name' in f and 'size' in f:
                f_name = f['name']
                f_size = f['size']
                f_path = os.path.join(dirpath, f_name)
                self.assertEqual(os.path.exists(f_path), fpath_exists)
                self.assertEqual(os.path.getsize(f_path), f_size)

    @unittest.skipUnless(os.path.exists(TESTS_DATA['cvmfs_path']), \
            'You do not have ATLAS cvmfs available. ' + \
            'DQ2 client installation is missing.')
    def test_download_with_dq2client(self, \
                test_file_config='test_file_exists'):
        """
            test_download_with_dq2client

            1. Obtain Rucio OAuth Token.
            2. Use DQ2 client to get PFN from guid,site,lfn,scope
            3. Fetch file with PFN
            4. Test that log directory and files exist and have expected sizes
        """
        ### test file properties
        lfn = TESTS_DATA[test_file_config]['lfn']
        scope = TESTS_DATA[test_file_config]['scope']
        guid = TESTS_DATA[test_file_config]['guid']
        site = TESTS_DATA[test_file_config]['site']
        ### rucio oauth token
        oauth_token = get_rucio_oauth_token()
        self.assertEqual(len(oauth_token) > 0, True)
        ### pfns
        pfns, errors = get_rucio_pfns_from_guids_with_dq2client(\
                        [guid, ], site, [lfn, ], [scope, ])
        self.assertEqual(len(errors) > 0, False)
        self.assertEqual(len(pfns) > 0, True)
        ### fetch file with PFN
        if len(pfns)>0:
            pfn = pfns[0]
        else:
            pfn='na'
        files, errtxt, dirprefix = fetch_file(pfn, guid)
        self.assertEqual(len(errors) > 0, False)
        self.assertEqual(len(files) > 0, True)
        self.assertEqual(len(dirprefix) > 0, True)
        ### test that the log tarball directory exists
        dirpath = getattr(settings, "MEDIA_ROOT", "/tmp") + '/' + dirprefix
        self.assertEqual(os.path.isdir(dirpath), True)
        self.assertEqual(len(os.listdir(dirpath)) > 0, True)
        ### test that the files exist and have expected size
        for f in files:
            if 'name' in f and 'size' in f:
                f_name = f['name']
                f_size = f['size']
                f_path = os.path.join(dirpath, f_name)
                self.assertEqual(os.path.exists(f_path), True)
                self.assertEqual(os.path.getsize(f_path), f_size)


    def test_download_with_rucio_metalink_file_forcefailure(self, \
                test_file_config='test_file_failure', \
                eval_len_metalink=False, eval_len_surls=False, \
                eval_len_errors_pfn=False, \
                eval_len_errors=False, eval_len_pfns=False, \
                eval_len_files=False, eval_len_dirprefix=True, \
                dirpath_exists=True, dirpath_listing=False, \
                fpath_exists=False):
        return self.test_download_with_rucio_metalink_file(test_file_config, \
                eval_len_metalink, eval_len_surls, \
                eval_len_errors_pfn, \
                eval_len_errors, eval_len_pfns, \
                eval_len_files, eval_len_dirprefix, \
                dirpath_exists, dirpath_listing, \
                fpath_exists)


    def test_download_with_rucio_redirect_forcefailure(self, \
                test_file_config='test_file_failure', \
                eval_len_redirectURL=True, eval_len_surls=False, \
                eval_len_errors_pfn=False, \
                eval_len_errors=False, eval_len_pfns=False, \
                eval_len_files=False, eval_len_dirprefix=True, \
                dirpath_exists=True, dirpath_listing=False, \
                fpath_exists=False):
        return self.test_download_with_rucio_redirect(test_file_config,
                eval_len_redirectURL, eval_len_surls, \
                eval_len_errors_pfn, \
                eval_len_errors, eval_len_pfns, \
                eval_len_files, eval_len_dirprefix, \
                dirpath_exists, dirpath_listing, \
                fpath_exists)


    @unittest.skipUnless(os.path.exists(TESTS_DATA['cvmfs_path']), \
            'You do not have ATLAS cvmfs available. ' + \
            'DQ2 client installation is missing.')
    def test_download_with_dq2client_forcefailure(self, \
                test_file_config='test_file_exists'):
        return self.test_download_with_dq2client(test_file_config)


