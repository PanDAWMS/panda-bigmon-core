"""
    filebrowser.utils
    
"""
import subprocess
import json
import logging
import os
import re
import time
import shutil
from django.conf import settings
from datetime import datetime


_logger = logging.getLogger('bigpandamon-filebrowser')
filebrowserDateTimeFormat = "%Y %b %d %H:%M:%S"

def get_filebrowser_vo():
    """
        get_filebrowser_vo
        
    """
    return getattr(settings, "FILEBROWSER_VO", "atlas")


def get_filebrowser_hostname():
    """
        get_filebrowser_hostname
        
    """
    return getattr(settings, "FILEBROWSER_HOSTNAME", subprocess.getoutput('hostname -f'))


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



def execute_cmd(cmd):
    """
        execute_cmd
    """
    if len(cmd) > 0:
        return subprocess.getstatusoutput(cmd)
    else:
        return subprocess.getstatusoutput('echo')


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


def is_directory_exist(logdir):
    """
        check if folder exists
    """
    if os.path.exists(os.path.dirname(logdir)):
        return True
    else:
        return False


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
    (status, output) = subprocess.getstatusoutput(cmd)
    if status != 0:
        msg = 'Cannot unpack file [%s].' % (fname)
        _logger.error(msg)
        _logger.error(str(output))
        errtxt = msg
    return status, errtxt


def list_file_directory(logdir, limit=1000):
    """
        list_file_directory
        
    """
    #_logger.error("list_file_directory started - " + datetime.now().strftime("%H:%M:%S") + "  " + logdir)

    files = []
    err = ''

    cmd = " ls -l %s " % (logdir)
    status, output = execute_cmd(cmd)

    # Process the tarball contents
    # First find the basename for the tarball files
    tardir = ''
    filelist = []
    try:
        filelist = os.listdir(logdir)
    except OSError as errMsg:
        msg = "Error in filesystem call:" + str(errMsg)
        _logger.error(msg)

    if len(filelist) > 0:
        for entry in filelist:
            if entry.startswith('tarball'):
                tardir = entry
                continue
    if not len(tardir):
        err = "Problem with tarball, could not find expected tarball directory. "
        _logger.warning('{} (got {}).'.format(err, logdir))
        # try to show any xml, json and txt files that are not in tarball directory
        filtered_filelist = []
        if len(filelist) > 0:
            for entry in filelist:
                if entry.endswith('.xml') or entry.endswith('.json') or entry.endswith('.txt'):
                    filtered_filelist.append(entry)
        if len(filtered_filelist) == 0:
            err += ' Tried to look for files in top dir, but found nothing'
            _logger.error('{} (got {}).'.format(err, logdir))
            _logger.debug('Contents of untared log file: \n {}'.format(filelist))
            return files, err, tardir
        else:
            err += "However, a few files has been found in a topdir, so showing them."
            _logger.info('A few files found in a topdir, will show them to user')

    # Now list the contents of the tarball directory:
    try:
        contents = []
        dobrake = False
        _logger.debug('Walking through directory:')
        for walk_root, walk_dirs, walk_files in os.walk(os.path.join(logdir, tardir), followlinks=False):
            #_logger.error("walk - " + datetime.now().strftime("%H:%M:%S") + "  " + os.path.join(logdir, tardir))
            for name in walk_files:
                contents.append(os.path.join(walk_root, name))
                if len(contents) > limit:
                    dobrake = True
                    _logger.debug('Number of files added to contents reached the limit : {}'.format(limit))
                    break
            if dobrake:
                break
        _logger.debug('Contents: \n {}'.format(contents))
        fileStats = {}
        linkStats = {}
        linkName = {}
        isFile = {}
        for f in contents:
            myFile = f
            isFile[f] = os.path.isfile(myFile)
            try:
                fileStats[f] = os.lstat(myFile)
            except OSError as errMsg:
                err += 'Warning: Error in lstat on %s (%s).\n' % (myFile, errMsg)
                fileStats[f] = None
            if os.path.islink(myFile):
                try:
                    linkName[f] = os.readlink(myFile)
                    linkStats[f] = os.stat(myFile)
                except OSError as errMsg:
                    err += 'Warning: Error in stat on linked file %s linked from %s (%s).\n' % (linkName[f], f, errMsg)
                    linkStats[f] = None
        for f in contents:
            f_content = {}
            if f in fileStats:
                if fileStats[f] is not None and f in isFile and isFile[f]:
                    f_content['modification'] = str(time.strftime(filebrowserDateTimeFormat, time.gmtime(fileStats[f][8])))
                    f_content['size'] = fileStats[f][6]
                    f_content['name'] = os.path.basename(f)
                    f_content['dirname'] = re.sub(os.path.join(logdir, tardir), '', os.path.dirname(f) + '/')
                    f_content['dirname'] = f_content['dirname'][:-1] if f_content['dirname'].endswith('/') else f_content['dirname']
            files.append(f_content)
            _logger.debug('Files to be shown: \n {}'.format(files))
    except OSError as errMsg:
        msg = "Error in filesystem call:" + str(errMsg)
        _logger.error(msg)

    ### sort the files - no need since DataTable plugin applied
    # files = sorted(files, key=lambda x: (str(x['dirname']).lower(), str(x['name']).lower()))

    #_logger.error("list_file_directory finished - " + datetime.now().strftime("%H:%M:%S") + "  " + logdir)


    if status != 0:
        return files, output, tardir
    else:
        return files, err, tardir


def get_rucio_file(scope,lfn, guid, unpack=True, listfiles=True, limit=1000):

    errtxt = ''
    files = []

    #_logger.error("get_rucio_file step1 - " + datetime.now().strftime("%H:%M:%S") + "  " + guid)

    ### logdir
    logdir = get_fullpath_filebrowser_directory() + '/' + guid.lower()
    #### basename for the file
    #base = os.path.basename(lfn)
    fname = '%s:%s' % (scope, lfn)
    fpath = '%s/%s/%s' % (logdir, scope, lfn)

    ### create directory for files of guid
    dir, err = create_directory(fpath)
    if not len(err):
        errtxt += err

    ### get command to copy file
    cmd = 'export RUCIO_ACCOUNT=%s; export X509_USER_PROXY=%s;export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase; source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh -3;source $ATLAS_LOCAL_ROOT_BASE/packageSetups/localSetup.sh rucio; rucio download --dir=%s %s:%s' % (get_rucio_account(),get_x509_proxy(),logdir,scope,lfn)

    if not len(cmd):
        _logger.warning('Command to fetch the file is empty!')

    ### download the file
    status, err = execute_cmd(cmd)
    if status != 0:
        msg = 'File download failed with command [%s]. Output: [%s].' % (cmd, err)
        _logger.error(msg)
        errtxt += msg

    #_logger.error("get_rucio_file step2 - " + datetime.now().strftime("%H:%M:%S") + "  " + guid)


    if unpack:
        ### untar the file
        status, err = unpack_file(fpath)
        if status != 0:
            msg = 'File unpacking failed for file [%s].' % (fname)
            _logger.error('{} File path is {}.'.format(msg, fpath))
            errtxt = '\n '.join([errtxt, msg])

    #_logger.error("get_rucio_file step2-1 - " + datetime.now().strftime("%H:%M:%S") + "  " + guid)


    tardir = ''
    files = ''
    if listfiles:
        ### list the files
        files, err, tardir = list_file_directory(dir, limit)
        if len(err):
            msg = 'File listing failed for file [%s]: [%s].' % (fname, err)
            _logger.error(msg)
            errtxt = '\n'.join([errtxt, msg])

    ### urlbase
    urlbase = get_filebrowser_directory() +'/'+ guid.lower()+'/'+scope

    #_logger.error("get_rucio_file step3 - " + datetime.now().strftime("%H:%M:%S") + "  " + guid)

    ### return list of files
    return files, errtxt, urlbase, tardir


def remove_folder(guid):

    ### logdir
    logdir = get_fullpath_filebrowser_directory() + '/' + guid.lower()

    ### check if folder exists and remove
    if os.path.isdir(logdir):
        shutil.rmtree(logdir)
    else:
        msg = 'Provided folder is not provided [{}]'.format(logdir)
        _logger.error(msg)

    return logdir
