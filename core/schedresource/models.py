"""
topology.models -- for Schedconfig and other topology-related objects

"""

from django.db import models
from core.settings.config import DB_SCHEMA_PANDA

# Create your models here.
class Schedconfig(models.Model):
    name = models.CharField(max_length=180, db_column='NAME')
    nickname = models.CharField(max_length=180, primary_key=True, db_column='NICKNAME')
    queue = models.CharField(max_length=180, db_column='QUEUE', blank=True)
    localqueue = models.CharField(max_length=60, db_column='LOCALQUEUE', blank=True)
    system = models.CharField(max_length=180, db_column='SYSTEM')
    sysconfig = models.CharField(max_length=60, db_column='SYSCONFIG', blank=True)
    environ = models.CharField(max_length=750, db_column='ENVIRON', blank=True)
    gatekeeper = models.CharField(max_length=120, db_column='GATEKEEPER', blank=True)
    jobmanager = models.CharField(max_length=240, db_column='JOBMANAGER', blank=True)
    ddm = models.CharField(max_length=360, db_column='DDM', blank=True)
    jdladd = models.CharField(max_length=1500, db_column='JDLADD', blank=True)
    globusadd = models.CharField(max_length=300, db_column='GLOBUSADD', blank=True)
    jdl = models.CharField(max_length=180, db_column='JDL', blank=True)
    jdltxt = models.CharField(max_length=1500, db_column='JDLTXT', blank=True)
    version = models.CharField(max_length=180, db_column='VERSION', blank=True)
    site = models.CharField(max_length=180, db_column='site')
    region = models.CharField(max_length=180, db_column='REGION', blank=True)
    gstat = models.CharField(max_length=180, db_column='GSTAT', blank=True)
    tags = models.CharField(max_length=600, db_column='TAGS', blank=True)
    cmd = models.CharField(max_length=600, db_column='CMD', blank=True)
    lastmod = models.DateTimeField(db_column='LASTMOD')
    errinfo = models.CharField(max_length=240, db_column='ERRINFO', blank=True)
    nqueue = models.IntegerField(db_column='NQUEUE')
    comment_field = models.CharField(max_length=1500, db_column='comment_', blank=True)  # Field renamed because it was a Python reserved word.
    appdir = models.CharField(max_length=1500, db_column='APPDIR', blank=True)
    datadir = models.CharField(max_length=240, db_column='DATADIR', blank=True)
    tmpdir = models.CharField(max_length=240, db_column='TMPDIR', blank=True)
    wntmpdir = models.CharField(max_length=240, db_column='WNTMPDIR', blank=True)
    dq2url = models.CharField(max_length=240, db_column='DQ2URL', blank=True)
    special_par = models.CharField(max_length=240, db_column='SPECIAL_PAR', blank=True)
    python_path = models.CharField(max_length=240, db_column='PYTHON_PATH', blank=True)
    nodes = models.IntegerField(db_column='NODES')
    status = models.CharField(max_length=30, db_column='status', blank=True)
    copytool = models.CharField(max_length=240, db_column='COPYTOOL', blank=True)
    releases = models.CharField(max_length=1500, db_column='RELEASES', blank=True)
    envsetup = models.CharField(max_length=600, db_column='ENVSETUP', blank=True)
    lfcpath = models.CharField(max_length=240, db_column='LFCPATH', blank=True)
    lfchost = models.CharField(max_length=240, db_column='LFCHOST', blank=True)
    cloud = models.CharField(max_length=180, db_column='cloud', blank=True)
    siteid = models.CharField(max_length=180, db_column='siteid', blank=True)
    proxy = models.CharField(max_length=240, db_column='PROXY', blank=True)
    retry = models.CharField(max_length=30, db_column='RETRY', blank=True)
    queuehours = models.IntegerField(db_column='QUEUEHOURS')
    envsetupin = models.CharField(max_length=600, db_column='ENVSETUPIN', blank=True)
    lfcprodpath = models.CharField(max_length=240, db_column='LFCPRODPATH', blank=True)
    recoverdir = models.CharField(max_length=240, db_column='RECOVERDIR', blank=True)
    memory = models.IntegerField(db_column='MEMORY')
    maxtime = models.IntegerField(db_column='MAXTIME')
    space = models.IntegerField(db_column='SPACE')
    tspace = models.DateTimeField(db_column='TSPACE')
    cmtconfig = models.CharField(max_length=750, db_column='CMTCONFIG', blank=True)
    glexec = models.CharField(max_length=30, db_column='GLEXEC', blank=True)
    priorityoffset = models.CharField(max_length=180, db_column='PRIORITYOFFSET', blank=True)
    allowedgroups = models.CharField(max_length=300, db_column='ALLOWEDGROUPS', blank=True)
    defaulttoken = models.CharField(max_length=300, db_column='DEFAULTTOKEN', blank=True)
    pcache = models.CharField(max_length=300, db_column='PCACHE', blank=True)
    validatedreleases = models.CharField(max_length=1500, db_column='VALIDATEDRELEASES', blank=True)
    accesscontrol = models.CharField(max_length=60, db_column='ACCESSCONTROL', blank=True)
    dn = models.CharField(max_length=300, db_column='DN', blank=True)
    email = models.CharField(max_length=180, db_column='EMAIL', blank=True)
    allowednode = models.CharField(max_length=240, db_column='ALLOWEDNODE', blank=True)
    maxinputsize = models.IntegerField(null=True, db_column='MAXINPUTSIZE', blank=True)
    timefloor = models.IntegerField(null=True, db_column='TIMEFLOOR', blank=True)
    depthboost = models.IntegerField(null=True, db_column='DEPTHBOOST', blank=True)
    idlepilotsupression = models.IntegerField(null=True, db_column='IDLEPILOTSUPRESSION', blank=True)
    pilotlimit = models.IntegerField(null=True, db_column='PILOTLIMIT', blank=True)
    transferringlimit = models.IntegerField(null=True, db_column='TRANSFERRINGLIMIT', blank=True)
    cachedse = models.IntegerField(null=True, db_column='CACHEDSE', blank=True)
    corecount = models.IntegerField(null=True, db_column='CORECOUNT', blank=True)
    countrygroup = models.CharField(max_length=192, db_column='COUNTRYGROUP', blank=True)
    availablecpu = models.CharField(max_length=192, db_column='AVAILABLECPU', blank=True)
    availablestorage = models.CharField(max_length=192, db_column='AVAILABLESTORAGE', blank=True)
    pledgedcpu = models.CharField(max_length=192, db_column='PLEDGEDCPU', blank=True)
    pledgedstorage = models.CharField(max_length=192, db_column='PLEDGEDSTORAGE', blank=True)
    statusoverride = models.CharField(max_length=768, db_column='STATUSOVERRIDE', blank=True)
    allowdirectaccess = models.CharField(max_length=30, db_column='ALLOWDIRECTACCESS', blank=True)
    gocname = models.CharField(max_length=192, db_column='gocname', blank=True)
    tier = models.CharField(max_length=45, db_column='tier', blank=True)
    multicloud = models.CharField(max_length=192, db_column='MULTICLOUD', blank=True)
    lfcregister = models.CharField(max_length=30, db_column='LFCREGISTER', blank=True)
    stageinretry = models.IntegerField(null=True, db_column='STAGEINRETRY', blank=True)
    stageoutretry = models.IntegerField(null=True, db_column='STAGEOUTRETRY', blank=True)
    fairsharepolicy = models.CharField(max_length=1536, db_column='FAIRSHAREPOLICY', blank=True)
    allowfax = models.CharField(null=True, max_length=64, db_column='ALLOWFAX', blank=True)
    faxredirector = models.CharField(null=True, max_length=256, db_column='FAXREDIRECTOR', blank=True)
    maxwdir = models.IntegerField(null=True, db_column='MAXWDIR', blank=True)
    celist = models.CharField(max_length=12000, db_column='CELIST', blank=True)
    minmemory = models.IntegerField(null=True, db_column='MINMEMORY', blank=True)
    maxmemory = models.IntegerField(null=True, db_column='MAXMEMORY', blank=True)
    minrss = models.IntegerField(null=True, db_column='MINRSS', blank=True)
    maxrss = models.IntegerField(null=True, db_column='MAXRSS', blank=True)
    mintime = models.IntegerField(null=True, db_column='MINTIME', blank=True)
    allowjem = models.CharField(null=True, max_length=64, db_column='ALLOWJEM', blank=True)
    catchall = models.CharField(null=True, max_length=512, db_column='catchall', blank=True)
    faxdoor = models.CharField(null=True, max_length=128, db_column='FAXDOOR', blank=True)
    wansourcelimit = models.IntegerField(null=True, db_column='WANSOURCELIMIT', blank=True)
    wansinklimit = models.IntegerField(null=True, db_column='WANSINKLIMIT', blank=True)
    auto_mcu = models.SmallIntegerField(null=True, db_column='AUTO_MCU', blank=True)
    objectstore = models.CharField(null=True, max_length=512, db_column='objectstore', blank=True)
    allowhttp = models.CharField(null=True, max_length=64, db_column='ALLOWHTTP', blank=True)
    httpredirector = models.CharField(null=True, max_length=256, db_column='HTTPREDIRECTOR', blank=True)
    multicloud_append = models.CharField(null=True, max_length=64, db_column='MULTICLOUD_APPEND', blank=True)
    corepower = models.IntegerField(null=True, db_column='corepower', blank=True)
    #Were added 21.12.17
    directaccesslan = models.CharField(null=True, max_length=64, db_column='DIRECT_ACCESS_LAN', blank=True)
    directaccesswan = models.CharField(null=True, max_length=64, db_column='DIRECT_ACCESS_WAN', blank=True)
    wnconnectivy = models.CharField(null=True, max_length=256, db_column='WNCONNECTIVITY', blank=True)
    cloudrshare = models.CharField(null=True, max_length=256, db_column='CLOUDRSHARE',  blank=True)
    sitershare = models.CharField(null=True, max_length=256, db_column='SITERSHARE',  blank=True)
    autosetup_post = models.CharField(null=True, max_length=512, db_column='AUTOSETUP_POST', blank=True)
    autosetup_pre = models.CharField(null=True, max_length=512, db_column='AUTOSETUP_PRE', blank=True)
    use_newmover = models.CharField(null=True, max_length=32, db_column='USE_NEWMOVER', blank=True)
    pilotversion = models.CharField(null=True, max_length=32, db_column='PILOTVERSION', blank=True)
    objectstores = models.CharField(null=True, max_length=4000, db_column='OBJECTSTORES', blank=True)
    container_options = models.CharField(null=True, max_length=1024, db_column='CONTAINER_OPTIONS',blank=True)
    container_type = models.CharField(null=True, max_length=256, db_column='CONTAINER_TYPE', blank=True)
    jobseed = models.CharField(null=True, max_length=16, db_column='JOBSEED', blank=True)
    pilot_manager = models.CharField(null=True, max_length=16, db_column='PILOT_MANAGER', blank=True)


    def __str__(self):
        return 'Schedconfig:' + str(self.nickname)

    def getFields(self):
        return ["name", "nickname", "queue", "localqueue", "system", \
                "sysconfig", "environ", "gatekeeper", "jobmanager",  "ddm", \
                "jdladd", "globusadd", "jdl", "jdltxt", "version", "site", \
                "region", "gstat", "tags", "cmd", "lastmod", "errinfo", \
                "nqueue", "comment_", "appdir", "datadir", "tmpdir", "wntmpdir", \
                "dq2url", "special_par", "python_path", "nodes", "status", \
                "copytool", "releases", "envsetup", \
                "lfcpath", "lfchost", \
                "cloud", "siteid", "proxy", "retry", "queuehours", "envsetupin", \
                "lfcprodpath", \
                "recoverdir", "memory", "maxtime", "space", \
                "tspace", "cmtconfig", "glexec", "priorityoffset", \
                "allowedgroups", "defaulttoken", "pcache", "validatedreleases", \
                "accesscontrol", "dn", "email", "allowednode", "maxinputsize", \
                 "timefloor", "depthboost", "idlepilotsupression", "pilotlimit", \
                 "transferringlimit", "cachedse", "corecount", "countrygroup", \
                 "availablecpu", "availablestorage", "pledgedcpu", \
                 "pledgedstorage", "statusoverride", "allowdirectaccess", \
                 "gocname", "tier", "multicloud", "lfcregister", "stageinretry", \
                 "stageoutretry", "fairsharepolicy", "allowfax", "faxredirector", \
                 "maxwdir", "celist", "minmemory", "maxmemory", "mintime", \
                 "allowjem", "catchall", "faxdoor", "wansourcelimit", \
                 "wansinklimit", "auto_mcu", "objectstore", "allowhttp", \
                 "httpredirector", "multicloud_append","direct_access_lan","direct_access_wan", \
                "wnconnectivy", "cloudrshare", "sitershare","autosetup_post","autosetup_pre","use_newmover","pilotversion" \
                "objectstores","container_options","container_type","jobseed","pilot_manager"
                ]

    def getValuesList(self):
        repre = []
        for field in self._meta.fields:
            repre.append((field.name, field))
        return repre


    def get_all_fields(self):
        """Returns a list of all field names on the instance."""
        fields = []
        kys = {}
        for f in self._meta.fields:
            kys[f.name] = f
        kys1 = kys.keys()
        kys1 = sorted(kys1)
        for k in kys1:
            f = kys[k]
            fname = f.name
            # resolve picklists/choices, with get_xyz_display() function
            get_choice = 'get_'+fname+'_display'
            if hasattr( self, get_choice):
                value = getattr( self, get_choice)()
            else:
                try :
                    value = getattr(self, fname)
                except User.DoesNotExist:
                    value = None

            # only display fields with values and skip some fields entirely
            if f.editable and value :

                fields.append(
                  {
                   'label':f.verbose_name, 
                   'name':f.name, 
                   'value':value,
                  }
                )
        return fields

    class Meta:
        db_table = u'schedconfig'


class SchedconfigJson(models.Model):
    pandaqueue = models.CharField(max_length=180, db_column='panda_queue', primary_key=True)
    data = models.TextField(db_column='data', blank=True)
    lastupdate = models.DateField(db_column='last_update')

    class Meta:
        db_table = f'"{DB_SCHEMA_PANDA}"."schedconfig_json"'


class Schedinstance(models.Model):
    name = models.CharField(max_length=180, db_column='NAME')
    nickname = models.CharField(max_length=180, db_column='NICKNAME', primary_key=True)
    pandasite = models.CharField(max_length=180, db_column='PANDASITE')
    nqueue = models.IntegerField(db_column='NQUEUE')
    nqueued = models.IntegerField(db_column='NQUEUED')
    nrunning = models.IntegerField(db_column='NRUNNING')
    nfinished = models.IntegerField(db_column='NFINISHED')
    nfailed = models.IntegerField(db_column='NFAILED')
    naborted = models.IntegerField(db_column='NABORTED')
    njobs = models.IntegerField(db_column='NJOBS')
    tvalid = models.DateTimeField(db_column='TVALID')
    lastmod = models.DateTimeField(db_column='LASTMOD')
    errinfo = models.CharField(max_length=450, db_column='ERRINFO', blank=True)
    ndone = models.IntegerField(db_column='NDONE')
    totrunt = models.IntegerField(db_column='TOTRUNT')
    comment_field = models.CharField(max_length=1500, db_column='COMMENT_', blank=True)  # Field renamed because it was a Python reserved word.
    class Meta:
        db_table = u'schedinstance'
        unique_together = ('nickname', 'pandasite')

