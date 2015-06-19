"""
    filebrowser.utils
    
"""
import commands
import json
import logging
import os
import re
import time
from django.conf import settings

_logger = logging.getLogger('bigpandamon-filebrowser')


def get_filebrowser_vo():
    """
        get_filebrowser_vo
        
    """
    return getattr(settings, "FILEBROWSER_VO", "atlas")


def get_filebrowser_hostname():
    """
        get_filebrowser_hostname
        
    """
    return getattr(settings, "FILEBROWSER_HOSTNAME", commands.getoutput('hostname -f'))


def get_filebrowser_directory():
    """
        get_filebrowser_directory
        
    """
    return getattr(settings, "FILEBROWSER_DIRECTORY", "filebrowser")


def get_fullpath_filebrowser_directory():
    """
        get_fullpath_filebrowser_directory
        
    """
    return getattr(settings, "MEDIA_ROOT", "/tmp") + '/' \
        + getattr(settings, "FILEBROWSER_DIRECTORY", "filebrowser")


def get_rucio_account():
    """
        get_rucio_account
        
    """
    return getattr(settings, "RUCIO_ACCOUNT", "atlpan")


def get_x509_proxy():
    """
        get_x509_proxy
        
    """
    return getattr(settings, \
                "X509_USER_PROXY", \
                "/data/atlpan/x509up_u25606")


def get_capath():
    """
        get_capath
        
    """
    return getattr(settings, \
                "CAPATH", \
                "/etc/grid-security/certificates")


def get_rucio_redirect_host():
    """
        get_rucio_redirect_host
        
    """
    return getattr(settings, \
                "RUCIO_REDIRECT_HOST", \
                "https://rucio-lb-prod.cern.ch")


def get_rucio_rest_api_auth_host():
    """
        get_rucio_rest_api_auth_host
        
    """
    return getattr(settings, \
                "RUCIO_AUTH_HOST", \
                "https://voatlasrucio-auth-prod.cern.ch")


def get_rucio_rest_api_server_host():
    """
        get_rucio_rest_api_server_host
        
    """
    return getattr(settings, \
                "RUCIO_SERVER_HOST", \
                "https://voatlasrucio-server-prod.cern.ch")


def get_my_settings():
    """
        get_my_settings()
        
        Get environment settings for the filebrowser.
        
    """
    my_settings = {}

    ### RUCIO_ACCOUNT="atlpan"
    my_settings['RUCIO_ACCOUNT'] = get_rucio_account()

    ### Rucio redirect host
    my_settings['RUCIO_REDIRECT_HOST'] = get_rucio_redirect_host()

    ### Rucio REST API Auth host
    my_settings['RUCIO_AUTH_HOST'] = get_rucio_rest_api_auth_host()

    ### Rucio REST API server host
    my_settings['RUCIO_SERVER_HOST'] = get_rucio_rest_api_server_host()

    my_settings['X509_USER_PROXY'] = get_x509_proxy()

    ### CAPATH
    my_settings['CAPATH'] = get_capath()

    return my_settings


def get_rucio_oauth_token():
    """
        get_rucio_oauth_token ... get Rucio OAuth Token
        
        return: the Rucio OAuth token
    """
    ### init the Rucio OAuth Token
    rucioToken = ''

    ### assemble the curl command
    cmd = ' curl -s -i -H "X-Rucio-Account: %(rucio_account)s" --cacert %(proxy)s --cert %(proxy)s --capath %(capath)s -X GET %(rucio_api_host)s/auth/x509_proxy ' \
        % {
           'rucio_account': get_rucio_account(), \
           'proxy': get_x509_proxy(), \
           'capath': get_capath(), \
           'rucio_api_host': get_rucio_rest_api_auth_host() \
         }
    cmd += ' | grep "X-Rucio-Auth-Token: " | sed -e "s#X-Rucio-Auth-Token: ##g" '
    _logger.info('get_rucio_oauth_token: cmd=(%s)' % cmd)

    ### get the curl command output
    status, output = commands.getstatusoutput(cmd)

    ### get the rucioToken
    rucioToken = output.rstrip()

    ### return the Rucio OAuth Token
    return rucioToken


def get_rucio_metalink_file(rucioToken, lfn, scope):
    """
        get_rucio_metalink_file ... get Rucio metalink file with replicas of the lfn
        @params
            rucioToken ... Rucio OAuth Token
            lfn ... file name, e.g. log.blahblah.tgz
            scope ... Rucio scope, e.g. "user", or "data13_8tev"
        
        returns: content of Rucio metalink file as json string 
    """
    ### get the metalink file for scope:lfn
    cmd = ' RUCIO_TOKEN="%(rucio_token)s" ;  curl -s --cacert %(proxy)s --capath %(capath)s  -H "X-Rucio-Auth-Token: $RUCIO_TOKEN"  -H "Accept: application/x-json-stream"  -X GET \'%(rucio_api_host)s/replicas/%(scope)s/%(lfn)s\' ' % \
        { \
            'rucio_api_host': get_rucio_rest_api_server_host(), \
            'proxy': get_x509_proxy(), \
            'capath': get_capath(), \
            'rucio_token': rucioToken, \
            'lfn': lfn, \
            'scope': scope, \
        }

    ### get the metalink file
    status, output = commands.getstatusoutput(cmd)

    ### return the Rucio metalink file
    return output


def get_surls_from_rucio_metalink_file(metalink):
    """
        get_surls_from_rucio_metalink_file ... get SURLs from Rucio metalink file
        @params
            metalink
        
        returns: list of SURLs
    """
    surls = []

    ### read data from metalink file
    data = {}
    try:
        data = json.loads('%s' % (metalink))
    except:
        _logger.error('get_surls_from_rucio_metalink_file: cannot load data from metalink file. Exiting.')
        return surls

    ### get rses node with surls
    rses = {}
    try:
        rses = data['rses']
    except:
        _logger.error('get_surls_from_rucio_metalink_file: cannot get rses node from metalink data. Exiting.')
        return surls

    ### collect list of surls from rses nodes
    for rse in rses:
        rseSurls = []
        try:
            rseSurls = rses[rse]
        except:
            _logger.warning('get_surls_from_rucio_metalink_file: cannot get SURLs for rse %s. Continue.' % rse)
        surls.extend(rseSurls)

    ### return list of surls
    return surls


def get_rucio_pfns_from_guids_with_rucio_metalink_file(guids, site, lfns, scopes):
    """ 
        get_rucio_pfns_from_guids_with_rucio_metalink_file 
            ... Get the Rucio replica dictionary from Rucio metalink file
        @params
            guids ... list of GUIDs
            site ... PanDA resource (siteID)
            lfns ... list of LFNs
            scopes ... list of scopes for the LFNs
        
        returns: list of PFNs
    """

    # FORMAT: { guid1: {'surls': [surl1, ..], 'lfn':LFN, 'fsize':FSIZE, 'checksum':CHECKSUM}, ..}
    # where e.g. LFN='mc10_7TeV:ESD.321628._005210.pool.root.1', FSIZE=110359950 (long integer), CHECKSUM='ad:7bfc5de9'
    # surl1='srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/atlas/atlasdatadisk/rucio/mc12_8TeV/cf/8f/EVNT.01365724._000001.pool.root.1'
    # guid1='28FB7AE9-2234-F644-962A-17EA1D279AA7'

    errtxt = ''
    pfnlist = []

    ### make sure RUCIO_ACCOUNT is in environment
    rucioAccount = get_rucio_account()

    ### make sure X509_USER_PROXY is in environment
    X509Proxy = get_x509_proxy()

    ### get Rucio OAuth token
    rucioOAuthToken = get_rucio_oauth_token()

    for lfn in lfns:
        ### get scope
        scope = 'ERROR_failed-to-determine-scope'
        try:
            scope = scopes[0]
        except:
            _logger.warning('get_rucio_pfns_from_guids_with_rucio_metalink_file: failed to determine scope. Using scope=' % (scope))

        ### get the metalink file for each lfn
        metalink = get_rucio_metalink_file(rucioOAuthToken, lfn, scope)

        ### get list of surls from the metalink file
        surls = get_surls_from_rucio_metalink_file(metalink)

        ### add surls to pfnlist
        if len(surls):
            pfnlist.extend(surls)

    ### make pfnlist unique
    try:
        pfnlist = list(set(pfnlist))
    except:
        _logger.warning('get_rucio_pfns_from_guids_with_rucio_metalink_file: failed to make pfnlist unique')

    return pfnlist, errtxt


def get_rucio_redirect_url(lfn, scope):
    """
        get_rucio_redirect_url: assemble Rucio redirect URL 
        @params: lfn ... one filename
                        e.g. user.gangarbt.62544955._2108356106.log.tgz
                  scope ... scope of the file with lfn
                        e.g. user.gangarbt, or valid1
        
        returns: the Rucio redirect URL
    """
    redirectUrl = ''
    ### compose the redirecURL
    redirectUrl = '%(redirecthost)s/redirect/%(scope)s/%(filename)s%(suffix)s' % \
            {\
                'redirecthost': get_rucio_redirect_host(), \
                'scope': scope, \
                'filename': lfn, \
                'suffix': '' \
            }
    _logger.info('get_rucio_redirect_url: redirectUrl=(%s)' % redirectUrl)
    ### return the redirectURL
    return redirectUrl


def get_location_from_rucio_redirect_output(output):
    """
        getLocationFromRucioRedirectOutput 
            output ... output of e.g. curl -i --silent --capath /etc/grid-security/certificates --cacert /data/atlpan/x509up_u25606 --cert /data/atlpan/x509up_u25606 https://voatlasrucio-redirect-prod-01.cern.ch/redirect/user.kkrizka/user.kkrizka.016447._2112520451.log.tgz
            
    """
    surl = ''
    ### get lines of the output
    lines = output.split('\n')
    stopProcessing = False
    for line in lines:
        if line.startswith('Location:') and not stopProcessing:
            stopProcessing = True
            words = line.split()
            try:
                surl = words[1]
            except:
                _logger.error('get_location_from_rucio_redirect_output: Cannot get surl from curl output.')
    return surl


def get_rucio_redirect_response(redirectUrl):
    """
        get_rucio_redirect_response: get response of Rucio redirect 
        @params: redirectUrl ... one URL
            e.g. https://voatlasrucio-redirect-prod-01.cern.ch/redirect/user.gangarbt/user.gangarbt.62544955._2108356106.log.tgz
        
    """
    if len(redirectUrl) < 1:
        return ''
    surl = ''
    ### command
    cmd = " curl -i --silent --capath %(capath)s --cacert %(x509proxy)s --cert %(x509proxy)s %(url)s " % \
        {\
            'capath': get_capath(), \
            'x509proxy': get_x509_proxy(), \
            'url': redirectUrl \
         }
    _logger.info('get_rucio_redirect_response: cmd=(%s)' % cmd)
    status, output = commands.getstatusoutput(cmd)
    surl = get_location_from_rucio_redirect_output(output)
    return surl


def get_rucio_pfns_from_guids_with_rucio_redirect(guids, site, lfns, scopes):
    """ 
        get_rucio_pfns_from_guids_with_rucio_redirect
        ... Get the Rucio replica dictionary from Rucio redirect
    """

    # FORMAT: { guid1: {'surls': [surl1, ..], 'lfn':LFN, 'fsize':FSIZE, 'checksum':CHECKSUM}, ..}
    # where e.g. LFN='mc10_7TeV:ESD.321628._005210.pool.root.1', FSIZE=110359950 (long integer), CHECKSUM='ad:7bfc5de9'
    # surl1='srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/atlas/atlasdatadisk/rucio/mc12_8TeV/cf/8f/EVNT.01365724._000001.pool.root.1'
    # guid1='28FB7AE9-2234-F644-962A-17EA1D279AA7'

    errtxt = ''
    pfnlist = []

    ### make sure RUCIO_ACCOUNT is in environment
    get_rucio_account()
    ### make sure X509_USER_PROXY is in environment
    get_x509_proxy()

    for lfn in lfns:
        surl = ''
        ### get scope
        scope = 'ERROR_failed-to-determine-scope'
        try:
            scope = scopes[0]
        except:
            _logger.warning('get_rucio_pfns_from_guids_with_rucio_redirect: failed to determine scope. Using scope=' % (scope))
        ### get Rucio redirect URL
        redirectUrl = get_rucio_redirect_url(lfn, scope)
        ### get pfnlist
        if len(redirectUrl) > 0:
            surl = get_rucio_redirect_response(redirectUrl)
        ### add surl to pfnlist
        if len(surl) > 0:
            pfnlist.append(surl)

    return pfnlist, errtxt


def get_rucio_pfns_from_guids_with_dq2client(guids, site, lfns, scopes):
    """ 
        get_rucio_pfns_from_guids_with_dq2client
        ... Get the Rucio replica dictionary from DQ2/Rucio client
    """

    # FORMAT: { guid1: {'surls': [surl1, ..], 'lfn':LFN, 'fsize':FSIZE, 'checksum':CHECKSUM}, ..}
    # where e.g. LFN='mc10_7TeV:ESD.321628._005210.pool.root.1', FSIZE=110359950 (long integer), CHECKSUM='ad:7bfc5de9'
    # surl1='srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/atlas/atlasdatadisk/rucio/mc12_8TeV/cf/8f/EVNT.01365724._000001.pool.root.1'
    # guid1='28FB7AE9-2234-F644-962A-17EA1D279AA7'

    fileDictionary = {}
    if len(guids) != len(lfns):
        return None

    for i in xrange(len(guids)):
        lfn = lfns[i]
        ### get scope
        scope = 'ERROR_failed-to-determine-scope'
        try:
            scope = scopes[0]
        except:
            _logger.warning('WARNING: get_rucio_pfns_from_guids_with_dq2client: failed to determine scope. Using scope=' % (scope))
        fileDictionary[guids[i]] = '%s:%s' % (scope, lfn)

    dictionaryReplicas = {}
    errtxt = ''
    pfnlist = []

    ### make sure RUCIO_ACCOUNT is in environment
    get_rucio_account()
    ### make sure X509_USER_PROXY is in environment
    get_x509_proxy()

    try:
        from dq2.filecatalog import create_file_catalog
        from dq2.filecatalog.FileCatalogException import FileCatalogException
        from dq2.filecatalog.FileCatalogUnavailable import FileCatalogUnavailable

    except:
        msg = "Failed to lookup your file! Unfortunately, import of the dq2 modules failed."
        _logger.error(msg)
        errtxt += msg
    else:
        try:
            cat = self.getRucioCatalogHost(site)
            catalog = create_file_catalog(cat)
            catalog.connect()
            dictionaryReplicas = catalog.bulkFindReplicas(fileDictionary)
            catalog.disconnect()
        except:
            msg = "Failed to lookup your file! " + \
                "The catalog was not configured properly with the input " + \
                "data you provided: guid=%s site=%s lfn=%s scope=%s. " % \
                (guids, site, lfns, scopes)
            _logger.error(msg)
            errtxt += msg
            msg = "Please note that scope, lfn, and guid are compulsory parameters! "
            _logger.error(msg)
            errtxt += msg

    for g in dictionaryReplicas.keys():
        try:
            pfnList = dictionaryReplicas[g]['surls']
            pfnlist.extend(pfnList)
        except:
            msg = "Failed to lookup your file! Cannot extract surls for RucioDictionaryReplica of guid:%s" % (g)
            _logger.error(msg)
            errtxt += msg

    return pfnlist, errtxt


def get_rucio_pfns_from_guids(guids, site, lfns, scopes):
    """ 
        Get the Rucio replica dictionary 
            from Rucio metalink file
            or
            from Rucio redirector
            or
            from DQ2/Rucio client 
    """
    pfnlist = []
    errtxt = ''

    ### get pfnlist from Rucio metalink file
    try:
        pfnlist, errtxt = get_rucio_pfns_from_guids_with_rucio_metalink_file(guids, site, lfns, scopes)
    except:
        _logger.error('Failed to get PFNS from metalink file: guids=%s, site=%s, lfns=%s, scopes=%s' \
                      % (guids, site, lfns, scopes))
    ### pfnlist from Rucio metalink file not empty, return it
    if len(pfnlist) > 0:
        return pfnlist, errtxt

    ### get pfnlist from Rucio redirector
    try:
        pfnlist, errtxt = get_rucio_pfns_from_guids_with_rucio_redirect(guids, site, lfns, scopes)
    except:
        _logger.error('Failed to get PFNS from redirect: guids=%s, site=%s, lfns=%s, scopes=%s' \
                      % (guids, site, lfns, scopes))
    ### pfnlist from Rucio redirector not empty, return it
    if len(pfnlist) > 0:
        return pfnlist, errtxt

    ### get pfnlist from DQ2/Rucio client
    try:
        pfnlist, errtxt = get_rucio_pfns_from_guids_with_dq2client(guids, site, lfns, scopes)
    except:
        _logger.error('Failed to get PFNS from DQ2 client: guids=%s, site=%s, lfns=%s, scopes=%s' \
                      % (guids, site, lfns, scopes))
    ### pfnlist from DQ2/Rucio client not empty, return it
    if len(pfnlist) > 0:
        return pfnlist, errtxt

#    errtxt += 'You are out of luck today, file not found. ' \
#            + 'Used search parameters: ' \
#            + 'site=' + str(site) + ' ' \
#            + 'guids=' + str(guids) + ' ' \
#            + 'lfns=' + str(lfns) + ' ' \
#            + 'scopes=' + str(scopes) + ' ' \
#            + errtxt
    errtxt = 'You are out of luck today, file lookup failed. ' + \
        'We have tried three different ways to find your file, unfortunately with no joy.'

    ### default return
    return pfnlist, errtxt


def execute_cmd(cmd):
    """
        execute_cmd
    """
    if len(cmd) > 0:
        return commands.getstatusoutput(cmd)
    else:
        return commands.getstatusoutput('echo')


def get_filename(fname, guid):
    """
        get_filename
        
    """
    ### logdir
    logdir = get_fullpath_filebrowser_directory() + '/' + guid.lower()
    ### basename for the file
    base = os.path.basename(fname)
    return '%s/%s' % (logdir, base)


def create_directory(fname):
    """
        create_directory
    """
    errtxt = ''
    ### logdir
    logdir = os.path.dirname(fname)
    cmd = """  mkdir -p %s """ % (logdir)
    status, err = execute_cmd(cmd)
    if status != 0:
        msg = 'Failed to create directory %s. %s' % (logdir, str(err))
        _logger.error(msg)
        errtxt = msg
    return logdir, errtxt


def get_copycmd(fname, guid):
    """
        get_copycmd
        
        returns command string to be executed
    """
    ### command to execute to fetch the file
    cmd = ''
    copycmd = ''
    ### logdir
    logdir = get_fullpath_filebrowser_directory() + '/' + guid.lower()
    ### Virtual Organisation
    vo = get_filebrowser_vo()
    ### basename for the file
    base = os.path.basename(fname)

    # Do we already have this logfile? Have a look for the tarball. If it is not there, then
    # we get it before we do anything else
#    if (not os.path.isfile("%s/%s" % (logdir, base))): #FIXME
    if True:
        # We don't have this tarball, let's go get it from the grid
        if (fname.startswith('srm:')):
            # See: http://ppewww.ph.gla.ac.uk/~fergusjk/howtolcg.html
            # This should cover 99% of all cases now
            # Some US sites are not in the BDII, so we need to construct the full SURL with endpoint
            # If the LFC entry has SFN in it then it must have the full SURL:
            if fname.find('SFN=') >= 0:
                copycmd += """lcg-cp  -b -T srmv2 -V %(vo)s '%(file)s' "file:///$PWD/%(base)s" """ % {'vo' : vo, 'file' : fname, 'base' : base }
            else:
                copycmd += """lcg-cp  -T srmv2 -V %(vo)s '%(file)s' "file:///$PWD/%(base)s" """ % {'vo' : vo, 'file' : fname, 'base' : base }

        elif  (fname.startswith('https:') and re.search('/rucio/', fname)):
            ### e.g.: https://lapp-se01.in2p3.fr:443/dpm/in2p3.fr/home/atlas/atlasscratchdisk/rucio/user/kkrizka/51/2a/user.kkrizka.016447._2112520451.log.tgz
            copycmd += " curl -s -L -O --cacert %(proxy)s --capath %(capath)s --cert %(proxy)s %(url)s " % \
                {
                    'proxy': get_x509_proxy(), \
                    'capath': get_capath(), \
                    'url': fname, \
                }
        # For non-SRM SURLS this will probably require debugging for CERN...
        elif fname.startswith('/pnfs/usatlas.bnl.gov'):
            ## replacing dccp with srmcp ##
            copycmd += 'srmcp -globus_tcp_port_range=20000,50000 "srm://dcsrm.%s:8443%s" file:///$PWD/%s' \
                % (fname.split('/', 3)[2], fname, base)
        elif fname.startswith('/pnfs/uchicago.edu'):
            copycmd += 'globus-url-copy "gsiftp://uct2-dc1.uchicago.edu%s" "file://$PWD/%s"' % (fname, base)
        elif fname.startswith('/pnfs/iu.edu'):
            copycmd += 'globus-url-copy "gsiftp://iut2-dc1.iu.edu%s" "file://$PWD/%s"' % (fname, base)
        elif fname.startswith('/pnfs/aglt2.org/atlasproddisk') or fname.startswith('/pnfs/aglt2.org/atlasuserdisk'):
            copycmd += 'globus-url-copy "gsiftp://umfs07.aglt2.org:2811%s" "file://$PWD/%s"' % (fname, base)
        elif fname.startswith('dcap:'):
            ## replacing dccp with srmcp ##
            copycmd += 'srmcp -globus_tcp_port_range=20000,50000 "srm://dcsrm.%s:8443%s" file:///$PWD/%s' \
                % (fname.split('/', 3)[2], fname, base)
        else:
            # Copy command of last resort...
            copycmd += 'globus-url-copy "%s" file://$PWD/%s' % (fname, base)

        # Now exectute the tarball fetcher...
        cmd = '%s ; %s ; cd %s; %s' % (\
                ' export X509_USER_PROXY=' + get_x509_proxy() + ' ', \
                ' export LCG_GFAL_INFOSYS=lcg-bdii.cern.ch:2170 ', \
                logdir, copycmd)
    
    return cmd


def unpack_file(fname):
    """
        unpack_file
    """
    errtxt = ''
    ### logdir
    logdir = os.path.dirname(fname)
    ### basename for the file
    base = os.path.basename(fname)

    cmd = "cd %s; tar -xvzf %s" % (logdir, base)
    (status, output) = commands.getstatusoutput(cmd)
    if status != 0:
        msg = 'Cannot unpack file [%s].' % (fname)
        _logger.error(msg)
        _logger.error(str(output))
        errtxt = msg
    return status, errtxt


def list_file_directory(logdir):
    """
        list_file_directory
        
    """
    files = []
    err = ''

    cmd = " ls -l %s " % (logdir)
    status, output = execute_cmd(cmd)

    # Process the tarball contents
    # First find the basename for the tarball files
    tardir = ''
    for entry in os.listdir(logdir):
        if entry.startswith('tarball'):
            tardir = entry
            continue
    if not len(tardir):
        err = "Problem with tarball, could not find expected directory (got %s)." % os.listdir(logdir)
        return files, err, tardir

    # Now list the contents of the tarball directory:
    try:
        contents = []
        for walk_root, walk_dirs, walk_files in \
            os.walk(os.path.join(logdir, tardir), followlinks=False):
            for name in walk_files:
                contents.append(os.path.join(walk_root, name))
        _logger.debug(contents)
        fileStats = {}
        linkStats = {}
        linkName = {}
        isFile = {}
        for f in contents:
            myFile = f
            isFile[f] = os.path.isfile(myFile)
            try:
                fileStats[f] = os.lstat(myFile)
            except OSError, (errno, errMsg):
                err += 'Warning: Error in lstat on %s (%s).\n' % (myFile, errMsg)
                fileStats[f] = None
            if os.path.islink(myFile):
                try:
                    linkName[f] = os.readlink(myFile)
                    linkStats[f] = os.stat(myFile)
                except OSError, (errno, errMsg):
                    err += 'Warning: Error in stat on linked file %s linked from %s (%s).\n' % (linkName[f], f, errMsg)
                    linkStats[f] = None
        for f in contents:
            f_content = {}
            if f in fileStats:
                if fileStats[f] is not None and f in isFile and isFile[f]:
                    f_content['modification'] = time.asctime(time.gmtime(fileStats[f][8]))
                    f_content['size'] = fileStats[f][6]
                    f_content['name'] = os.path.basename(f)
                    f_content['dirname'] = re.sub(os.path.join(logdir, tardir), '', os.path.dirname(f))
            files.append(f_content)
    except OSError, (errno, errMsg):
        msg = "Error in filesystem call:" + str(errMsg)
        _logger.error(msg)

    ### sort the files
    files = sorted(files, key=lambda x: (str(x['dirname']).lower(), str(x['name']).lower()))

    if status != 0:
        return files, output, tardir
    else:
        return files, '', tardir


def fetch_file(pfn, guid, unpack=True, listfiles=True):
    """
        fetch_file
            ... Download the file with pfn.
        
        returns: local path to the file
    """
    errtxt = ''
    files = []

    ### get the filename
    fname = get_filename(pfn, guid)

    ### create directory for files of guid
    dir, err = create_directory(fname)
    if not len(err):
        errtxt += err

    ### get command to copy file
    cmd = get_copycmd(pfn, guid)
    if not len(cmd):
        _logger.warning('Command to fetch the file is empty!')

    ### download the file
    status, err = execute_cmd(cmd)
    if status != 0:
        msg = 'File download failed with command [%s]. Output: [%s].' % (cmd, err)
        _logger.error(msg)
        errtxt += msg

    if unpack:
        ### untar the file
        status, err = unpack_file(fname)
        if status != 0:
            msg = 'File unpacking failed for file [%s].' % (fname)
            _logger.error(msg)

    tardir = ''
    files = ''
    if listfiles:
        ### list the files
        files, err, tardir = list_file_directory(dir)
        if len(err):
            msg = 'File listing failed for file [%s]: [%s].' % (fname, err)
            _logger.error(msg)

    ### urlbase
    urlbase = get_filebrowser_directory() + '/' + guid.lower()

    ### return list of files
    return files, errtxt, urlbase, tardir


