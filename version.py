""" 
    core.version 
"""
import commands
import ConfigParser
import json
import os
from datetime import datetime

def get_version_base__release_type__provides():
    """
        get_version_base ... get version string base from the setup.cfg
    """
    config = ConfigParser.ConfigParser()
    config.read(os.path.dirname(os.path.realpath(__file__)) + '/setup.cfg')
    version_base = config.get("global", "version")
    release_type = config.get("global", "release_type")
    provides = config.get("bdist_rpm", "provides")
    return (version_base, release_type, provides,)


def get_git_version():
    """
        get version of this git commit
    """
    dir = os.path.dirname(os.path.realpath(__file__))
    ### count cap of total number of commits in this repository
    ncommits = 1
    try:
        ncommits = commands.getoutput('git shortlog | wc -l')
    except:
        pass
    ### get last revision ID, short version
    last_rev_id = ''
    try:
        last_rev_id = commands.getoutput('git show -s --pretty=format:%h ')
    except:
        pass
    ### get number of revisions on this branch
    nrevs = 1
    try:
        nrevs = commands.getoutput('git reflog | wc -l')
    except:
        pass
    if ncommits < 19000:
        retstr = str('.dev-' + ncommits + '-' + last_rev_id + '-' + nrevs)
    else:
        retstr = str('.dev-' + nrevs + '-' + last_rev_id + '-' + ncommits + nrevs)
    return retstr


def get_version_provides():
    isStable = False
    version_base, release_type, provides = get_version_base__release_type__provides()
    if release_type == 'stable':
        isStable = True
    __version__ = version_base
    if os.environ.has_key('BUILD_NUMBER'):
        __version__ = '{0}.{1}'.format(version_base, os.environ['BUILD_NUMBER'])
    if not isStable:
        __version__ += get_git_version()
    __provides__ = provides
    return (__version__, __provides__,)


def dump_version_string(version_string, provides):
    dumpfile_dir = os.path.dirname(os.path.realpath(__file__)) + '/core/media/'
    dumpfile = os.path.join(dumpfile_dir, 'version.json')
    ### check that the directory for dumpfile exists
    if not os.path.exists(dumpfile_dir):
        status, output = commands.getstatusoutput(" mkdir -p %s " % (dumpfile_dir))
        if status != 0:
            dumpfile = 'version.json'
    ### prepare version data
    data = {}
    data['version'] = str(version_string)
    data['provides'] = str(provides)
    data['created'] = datetime.isoformat(datetime.utcnow())
    ### dump data to dumpfile
    f = open(dumpfile, 'w')
    f.write(json.dumps(data, sort_keys=True, indent=4))
    f.close()
    ### return dumpfile name
    return dumpfile


__version__, __provides__ = get_version_provides()



