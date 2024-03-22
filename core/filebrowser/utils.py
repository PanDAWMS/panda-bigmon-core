"""
    filebrowser.utils
    
"""
import subprocess
import logging
import os
import re
import time
import shutil
from datetime import datetime
from django.conf import settings
from core.filebrowser.ObjectStoreWrapper import ObjectStore
from core.common.models import Filestable4, FilestableArch
from core.libs.job import get_job_list
from core.libs.exlib import convert_bytes
from core.schedresource.utils import get_panda_queues

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


def is_file_exist(path):
    """
        check if folder exists
    """
    if os.path.isfile(path):
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


def list_file_directory(logdir, limit=1000, log_provider='rucio'):
    """
        list_file_directory
        
    """
    _logger.debug("list_file_directory started - " + datetime.now().strftime("%H:%M:%S") + "  " + logdir)

    files = []
    err = ''
    tardir = ''

    cmd = " ls -l {} ".format(logdir)
    status, output = execute_cmd(cmd)
    if status != 0:
        err += 'Failed "{}" with: \n{} '.format(cmd, output)
        _logger.info(err)
        return files, err, tardir

    # Process the tarball contents
    # First find the basename for the tarball files
    filelist = []
    try:
        filelist = os.listdir(logdir)
    except OSError as errMsg:
        msg = "Error in filesystem call:" + str(errMsg)
        _logger.error(msg)

    if log_provider == 'rucio':
        # looking for tarball directory
        if len(filelist) > 0:
            for entry in filelist:
                if entry.startswith('tarball'):
                    tardir = entry
                    continue

        if not len(tardir):
            err = "Problem with tarball, could not find expected tarball directory. "
            _logger.info('{} (got {}).'.format(err, logdir))
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
            _logger.debug("walk - {}".format(os.path.join(logdir, tardir)))
            for name in walk_files:
                contents.append(os.path.join(walk_root, name))
                if len(contents) > limit:
                    dobrake = True
                    _logger.info('Number of files added to contents reached the limit : {}'.format(limit))
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

    return files, err, tardir


def get_rucio_file(scope, lfn, guid, unpack=True, listfiles=True, limit=1000):
    """
    Get logs with rucio
    :param scope: str
    :param lfn: str
    :param guid: str
    :param unpack: boolean
    :param listfiles: boolean
    :param limit: int - number of files to be listed and return in files
    :return: files: list - log files in local directory
    :return: err_txt: str -  error message
    :return: urlbase: str - base path for links to files
    :return: tardir: str - name of directory in unpacked tar
    """
    err_txt = ''
    tardir = ''
    files = []
    _logger.debug("get_rucio_file start - " + datetime.now().strftime("%H:%M:%S") + "  " + guid)

    # logdir
    logdir = '{}/{}'.format(get_fullpath_filebrowser_directory(), guid.lower())
    fname = '{}:{}'.format(scope, lfn)
    fpath = '{}/{}/{}'.format(logdir, scope, lfn)
    # urlbase
    urlbase = '{}/{}/{}/'.format(get_filebrowser_directory(), guid.lower(), scope)

    # create directory for files of guid
    dir, err = create_directory(fpath)
    if len(err) > 0:
        err_txt += err

    # get command to copy file
    cmd = 'export RUCIO_ACCOUNT=%s; export X509_USER_PROXY=%s;export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase; source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh; source $ATLAS_LOCAL_ROOT_BASE/packageSetups/localSetup.sh "python pilot-default"; source $ATLAS_LOCAL_ROOT_BASE/packageSetups/localSetup.sh rucio; rucio download --dir=%s %s:%s' % (get_rucio_account(),get_x509_proxy(),logdir,scope,lfn)
    if not len(cmd):
        _logger.warning('Command to fetch the file is empty!')

    # download the file
    status, err = execute_cmd(cmd)
    if status != 0:
        msg = 'File download failed with command [%s]. Output: [%s].' % (cmd, err)
        _logger.error(msg)
        if 'No valid proxy present' in err or 'certificate expired' in err:
            _logger.error("Internal Server Error: x509 proxy expired, can not connect to rucio")
        err_txt += '\n '.join([err_txt, msg])
    _logger.debug("get_rucio_file rucio download - " + datetime.now().strftime("%H:%M:%S") + "  " + guid)

    if unpack and len(err_txt) == 0:
        # untar the file
        status, err = unpack_file(fpath)
        if status != 0:
            msg = 'File unpacking failed for file [%s].' % (fname)
            _logger.error('{} File path is {}.'.format(msg, fpath))
            err_txt = '\n '.join([err_txt, msg])
        _logger.debug("get_rucio_file untar - " + datetime.now().strftime("%H:%M:%S") + "  " + guid)

    if listfiles and len(err_txt) == 0:
        # list the files
        files, err, tardir = list_file_directory(dir, limit)
        if len(err):
            msg = 'File listing failed for file [%s]: [%s].' % (fname, err)
            _logger.error(msg)
            err_txt = '\n'.join([err_txt, msg])
        _logger.debug("get_rucio_file files listed - " + datetime.now().strftime("%H:%M:%S") + "  " + guid)

    _logger.debug("get_rucio_file finished - " + datetime.now().strftime("%H:%M:%S") + "  " + guid)

    return files, err_txt, urlbase, tardir


def get_s3_file(pandaid, computingsite):
    """
    Getting logs from s3 ObjectStore
    :param pandaid: int
    :param computingsite: str
    :return: files: list - log files in local directory
    :return: err_txt: str -  error message
    :return: url_base: str - base path for links to files
    """
    err_txt = ''
    files = []

    s3 = ObjectStore()
    s3.init_resource()

    local_dir = '{}/{}'.format(get_fullpath_filebrowser_directory(), s3.get_folder_path(pandaid, computingsite))
    dirprefix = '{}/{}'.format(get_filebrowser_directory(), s3.get_folder_path(pandaid, computingsite))
    if not is_directory_exist(local_dir):
        dir, err = create_directory(local_dir)
        if len(err) > 0:
            err_txt += 'Failed to create local directory to copy logs to it. '

    error_download = s3.download_folder(
        s3_folder=s3.get_folder_path(pandaid, computingsite),
        local_dir=local_dir
    )
    if error_download:
        err_txt += error_download

    # list the files
    files, err, tardir = list_file_directory(local_dir, limit=100, log_provider='s3')
    if len(err):
        msg = 'File listing failed for dir [{}]: [{}].'.format(local_dir, err)
        _logger.error(msg)

    return files, err_txt, dirprefix, local_dir


def get_job_log_file_properties(pandaid=None, lfn=None, fileid=None):
    """
    Get log tarball file properties for PanDA job
    :param pandaid: int
    :param lfn: str
    :param fileid: int
    :return: scope: str
    :return: lfn: str
    :return: guid: str
    :return: fsize_mb: float
    """
    scope = None
    lfn = None
    guid = None
    fsize_mb = 0

    if not pandaid and not lfn and not fileid:
        _logger.debug('No params provided to get file properties')
        return scope, lfn, guid, pandaid, fsize_mb

    query = {
        'type': 'log',
    }
    if pandaid:
        query['pandaid'] = pandaid
    if lfn:
        query['lfn'] = lfn
    if fileid:
        query['fileid'] = fileid

    values = ['pandaid', 'guid', 'scope', 'lfn', 'fsize']
    file_properties = []
    file_properties.extend(Filestable4.objects.filter(**query).values(*values))
    if len(file_properties) == 0:
        file_properties.extend(FilestableArch.objects.filter(**query).values(*values))

    if len(file_properties):
        file_properties = file_properties[0]
        try:
            guid = file_properties['guid']
        except:
            pass
        try:
            lfn = file_properties['lfn']
        except:
            pass
        try:
            scope = file_properties['scope']
        except:
            pass
        if not pandaid:
            try:
                pandaid = file_properties['scope']
            except:
                pass
        try:
            fsize_mb = round(convert_bytes(int(file_properties['fsize']), output_unit='MB'))
        except:
            pass

    return scope, lfn, guid, pandaid, fsize_mb


def get_job_computingsite(pandaid):
    """
    Get computing site where job run
    :param pandaid: int
    :return: computingsite: str or None
    """
    computingsite = None
    joblist = get_job_list(query={"pandaid": pandaid})
    if joblist and len(joblist) > 0:
        computingsite = joblist[0].get('computingsite')
    return  computingsite


def get_log_provider(pandaid=None):
    """
    Figure out log provider for a job depending on settings.
    The default one is rucio. Also s3 object store is supported.
    In case it is 'cric', we get it from PanDA queues config.
    :param pandaid: int - optional
    :return:
    """
    log_provider = 'rucio'
    if hasattr(settings, 'LOGS_PROVIDER'):
        if settings.LOGS_PROVIDER == 'cric':
            # check cric config for computingsite
            if pandaid:
                computingsite = get_job_computingsite(pandaid)
                pqs = get_panda_queues()
                if computingsite in pqs and 'acopytools' in pqs[computingsite] and 'pw' in pqs[computingsite]['acopytools']:
                    pw = pqs[computingsite]['acopytools']['pw'][0]
                    if pw in ('rucio', 's3', 'gs'):
                        log_provider = pw
        else:
            log_provider = settings.LOGS_PROVIDER
    else:
        log_provider = 'rucio'
    _logger.info('Log provider is {}'.format(log_provider))
    return log_provider


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


def get_job_log_file_path(pandaid, filename=''):
    """
    Download log tarball of a job and return path to a local copy of a file
    If the directIO is enabled for prmon, return the remote location
    :param pandaid:
    :param filename: str, if empty the function return path to tarball folder
    :return: file_path: str or None
    """
    file_path = None
    files = []
    tarball_path = ''
    tardir = ''
    log_provider = get_log_provider(pandaid=pandaid)

    if log_provider == 'gs' and settings.PRMON_LOGS_DIRECTIO_LOCATION :
        computingsite = get_job_computingsite(pandaid)
        return settings.PRMON_LOGS_DIRECTIO_LOCATION.format(queue_name=computingsite, panda_id=pandaid) + '/' + filename

    if log_provider == 's3':
        computingsite = get_job_computingsite(pandaid)
        files, errtxt, dirprefix, tarball_path = get_s3_file(pandaid, computingsite)
        tardir = ''
    elif log_provider == 'rucio':
        scope, lfn, guid, _, fsize_mb = get_job_log_file_properties(pandaid)
        if guid and lfn and scope:
            # check if files are already available in common CEPH storage
            tarball_path = get_fullpath_filebrowser_directory() + '/' + guid.lower() + '/' + scope + '/'
            files, err, tardir = list_file_directory(tarball_path, 100)
            _logger.debug('tarball path is {} \nError message is {} \nGot tardir: {}'.format(tarball_path, err, tardir))
            if len(files) == 0 and len(err) > 0:
                # check size of tarball and download it if it less than 1GB - protection against huge load
                if fsize_mb and fsize_mb > 1024:
                    _logger.error('Size of log tarball too big to download')
                else:
                    _logger.debug('log tarball has not been downloaded, so downloading it now')
                    files, errtxt, dirprefix, tardir = get_rucio_file(scope, lfn, guid)
                    _logger.debug('Got files for dir: {} and tardir: {}. Error message: {}'.format(dirprefix, tardir, errtxt))

    if type(files) is list and len(files) > 0 and len(filename) > 0:
        for f in files:
            if f['name'] == filename:
                file_path = os.path.join(tarball_path, tardir , filename)
                continue

    _logger.debug('Final path of {} file: {}'.format(filename, file_path))
    return file_path