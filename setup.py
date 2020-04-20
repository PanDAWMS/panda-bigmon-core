#!/usr/bin/env python
#
# Setup prog for bigpandamon-core
#
#
from version import __version__, __provides__

import os
import re
import sys
import socket
import commands
import ConfigParser
from distutils.core import setup
from distutils.command.install import install as install_org
from distutils.command.install_data import install_data as install_data_org

# read config file setup.cfg
config = ConfigParser.ConfigParser()
config.read(os.path.dirname(os.path.realpath(__file__)) + '/setup.cfg')

# get prefix, lib_prefix, expected_extensions, src_ext, ignore_dir
prefix = config.get("config", "prefix")
lib_prefix = config.get("config", "lib_prefix")
expected_extensions = re.sub(' ', '', config.get("config", "expected_extensions")).split(',')
src_ext = re.sub(' ', '', config.get("config", "src_ext")).split(',')
ignore_dir = re.sub(' ', '', config.get("config", "ignore_dir")).split(',')

# get description, long_description, license, author, author_email, url
description = config.get("config", "description")
long_description = config.get("config", "long_description")
license = config.get("config", "license")
author = config.get("config", "author")
author_email = config.get("config", "author_email")
url = config.get("config", "url")

# get packages
packages = re.sub(' ', '', config.get("config", "packages")).split(',')

# get data_files
data_files_configs = re.sub(' ', '', config.get("config", "data_files_configs")).split(',')
data_files = []
try:
    data_files = [ ('%s%s' % (lib_prefix, x.split(':')[0]), [x.split(':')[1]]) \
                  for x in data_files_configs ]
except:
    data_files = []
data_files_templates = re.sub(' ', '', config.get("config", "data_files_templates")).split(',')


# get panda specific params
optPanda = {}
newArgv  = []
idx = 0
while idx < len(sys.argv):
    tmpArg = sys.argv[idx]
    if tmpArg.startswith('--panda_'):
        # panda params
        idx += 1            
        if len(tmpArg.split('=')) == 2:
            # split to par and val if = is contained
            tmpVal = tmpArg.split('=')[-1]
            tmpArg = tmpArg.split('=')[0]
        elif len(tmpArg.split('=')) == 1:
            tmpVal = sys.argv[idx]
            idx += 1
        else:
            raise RuntimeError,"invalid panda option : %s" % tmpArg
        # get key             
        tmpKey = re.sub('--panda_','',tmpArg)
        # set params
        optPanda[tmpKey] = tmpVal
    else:
        # normal opts
        idx += 1
        newArgv.append(tmpArg)
# set new argv
sys.argv = newArgv


# set overall prefix for bdist_rpm
class install_panda(install_org):
    def initialize_options (self):
        install_org.initialize_options(self)
        self.prefix = prefix


# generates files using templates and install them
class install_data_panda (install_data_org):

    def initialize_options (self):
        install_data_org.initialize_options (self)
        self.install_purelib = None
        self.host_name = socket.getfqdn()
        self.python_exec_version = '%s.%s' % sys.version_info[:2]
        
    def finalize_options (self):
        # set install_purelib
        self.set_undefined_options('install',
                                   ('install_purelib','install_purelib'))
        # set reaming params
        install_data_org.finalize_options(self)
        # set hostname
        if optPanda.has_key('hostname') and optPanda['hostname'] != '':
            self.hostname = optPanda['hostname']
        else:
            self.hostname = commands.getoutput('hostname -f')
        # set user and group
        if optPanda.has_key('username') and optPanda['username'] != '':
            self.username  = optPanda['username']
        else:
            self.username  = commands.getoutput('id -un')
        if optPanda.has_key('usergroup') and optPanda['usergroup'] != '':
            self.usergroup = optPanda['usergroup']
        else:
            self.usergroup = commands.getoutput('id -gn')             
        
    
    def is_expected_extension(self, filename):
        res = False
        for ext in expected_extensions:
            if not res and filename.endswith(ext):
                res = True
        return res


    def is_src_extension(self, filename):
        res = False
        for ext in src_ext:
            if not res and filename.endswith(ext):
                res = True
        return res


    def run (self):
        # remove /usr for bdist/bdist_rpm
        match = re.search('(build/[^/]+/dumb)/usr',self.install_dir)
        if match != None:
            self.install_dir = re.sub(match.group(0),match.group(1),self.install_dir)
        # remove /var/tmp/*-buildroot for bdist_rpm
        match = re.search('(/var/tmp/.*-buildroot)/usr',self.install_dir)
        if match != None:
            self.install_dir = re.sub(match.group(0),match.group(1),self.install_dir)
        # create tmp area
        tmpDir = 'build/tmp'
        self.mkpath(tmpDir)
        new_data_files = []
        for destDir,dataFiles in self.data_files:
            newFilesList = []
            for srcFile in dataFiles:
                # check extension
                if not srcFile.endswith('-template') and not self.is_expected_extension(srcFile):
                    raise RuntimeError,"%s doesn't have the -template extension" % srcFile
                # dest filename
                destFile = re.sub('(\.exe)*\-template$','',srcFile)
                destFile = destFile.split('/')[-1]
                destFile = '%s/%s/%s' % (tmpDir,srcFile,destFile)
                # open src
                inFile = open(srcFile)
                # read
                filedata=inFile.read()
                # close
                inFile.close()
                # replace patterns
                if not self.is_expected_extension(srcFile):
                    for item in re.findall('@@([^@]+)@@',filedata):
                        if not hasattr(self,item) and not self.is_expected_extension(srcFile):
                            raise RuntimeError,'unknown pattern %s in %s' % (item,srcFile)
                        # get pattern
                        patt = getattr(self,item)
                        # remove build/*/dump for bdist
                        patt = re.sub('build/[^/]+/dumb','',patt)
                        # remove /var/tmp/*-buildroot for bdist_rpm
                        patt = re.sub('/var/tmp/.*-buildroot','',patt)
                        # replace
                        filedata = filedata.replace('@@%s@@' % item, patt)
                # write to dest
                if not os.path.exists(os.path.dirname(destFile)):
                    os.makedirs(os.path.dirname(destFile))
                oFile = open(destFile,'w')
                oFile.write(filedata)
                oFile.close()
                # chmod for exe
                if srcFile.endswith('.exe-template'):
                    commands.getoutput('chmod +x %s' % destFile)
                # append
                newFilesList.append(destFile)
            # replace dataFiles to install generated file
            new_data_files.append((destDir,newFilesList))
        # install
        self.data_files = new_data_files
        install_data_org.run(self)


def gen_data_files(*dirs):
    """
    gen_data_files: generate list of files in the list of directories "dirs" 
    
    """
    results = []
    for src_dir in dirs:
        for root,dirs,files in os.walk(src_dir):
            results.append((lib_prefix + root, map(lambda f:root + "/" + f, files)))
    return results

data_files += gen_data_files(*data_files_templates)

# setup for distutils
setup(
    name=__provides__,
    version=__version__,
    description=description,
    long_description=long_description,
    license=license,
    author=author,
    author_email=author_email,
    url=url,
    packages=packages,
    data_files=data_files,
    cmdclass={'install': install_panda,
              'install_data': install_data_panda}
)

