"""
topology.models -- for Schedconfig and other topology-related objects

"""

from django.db import models
from django.conf import settings


class Schedconfig(models.Model):
    name = models.CharField(max_length=180, db_column='name')
    nickname = models.CharField(max_length=180, primary_key=True, db_column='nickname')
    queue = models.CharField(max_length=180, db_column='queue', blank=True)
    localqueue = models.CharField(max_length=60, db_column='localqueue', blank=True)
    system = models.CharField(max_length=180, db_column='system')
    sysconfig = models.CharField(max_length=60, db_column='sysconfig', blank=True)
    environ = models.CharField(max_length=750, db_column='environ', blank=True)
    gatekeeper = models.CharField(max_length=120, db_column='gatekeeper', blank=True)
    jobmanager = models.CharField(max_length=240, db_column='jobmanager', blank=True)
    ddm = models.CharField(max_length=360, db_column='ddm', blank=True)
    jdladd = models.CharField(max_length=1500, db_column='jdladd', blank=True)
    globusadd = models.CharField(max_length=300, db_column='globusadd', blank=True)
    jdl = models.CharField(max_length=180, db_column='jdl', blank=True)
    jdltxt = models.CharField(max_length=1500, db_column='jdltxt', blank=True)
    version = models.CharField(max_length=180, db_column='version', blank=True)
    site = models.CharField(max_length=180, db_column='site')
    region = models.CharField(max_length=180, db_column='region', blank=True)
    gstat = models.CharField(max_length=180, db_column='gstat', blank=True)
    tags = models.CharField(max_length=600, db_column='tags', blank=True)
    cmd = models.CharField(max_length=600, db_column='cmd', blank=True)
    lastmod = models.DateTimeField(db_column='lastmod')
    errinfo = models.CharField(max_length=240, db_column='errinfo', blank=True)
    nqueue = models.IntegerField(db_column='nqueue')
    comment_field = models.CharField(max_length=1500, db_column='comment_', blank=True)  # Field renamed because it was a Python reserved word.
    appdir = models.CharField(max_length=1500, db_column='appdir', blank=True)
    datadir = models.CharField(max_length=240, db_column='datadir', blank=True)
    tmpdir = models.CharField(max_length=240, db_column='tmpdir', blank=True)
    wntmpdir = models.CharField(max_length=240, db_column='wntmpdir', blank=True)
    dq2url = models.CharField(max_length=240, db_column='dq2url', blank=True)
    special_par = models.CharField(max_length=240, db_column='special_par', blank=True)
    python_path = models.CharField(max_length=240, db_column='python_path', blank=True)
    nodes = models.IntegerField(db_column='nodes')
    status = models.CharField(max_length=30, db_column='status', blank=True)
    copytool = models.CharField(max_length=240, db_column='copytool', blank=True)
    releases = models.CharField(max_length=1500, db_column='releases', blank=True)
    envsetup = models.CharField(max_length=600, db_column='envsetup', blank=True)
    lfcpath = models.CharField(max_length=240, db_column='lfcpath', blank=True)
    lfchost = models.CharField(max_length=240, db_column='lfchost', blank=True)
    cloud = models.CharField(max_length=180, db_column='cloud', blank=True)
    siteid = models.CharField(max_length=180, db_column='siteid', blank=True)
    proxy = models.CharField(max_length=240, db_column='proxy', blank=True)
    retry = models.CharField(max_length=30, db_column='retry', blank=True)
    queuehours = models.IntegerField(db_column='queuehours')
    envsetupin = models.CharField(max_length=600, db_column='envsetupin', blank=True)
    lfcprodpath = models.CharField(max_length=240, db_column='lfcprodpath', blank=True)
    recoverdir = models.CharField(max_length=240, db_column='recoverdir', blank=True)
    memory = models.IntegerField(db_column='memory')
    maxtime = models.IntegerField(db_column='maxtime')
    space = models.IntegerField(db_column='space')
    tspace = models.DateTimeField(db_column='tspace')
    cmtconfig = models.CharField(max_length=750, db_column='cmtconfig', blank=True)
    glexec = models.CharField(max_length=30, db_column='glexec', blank=True)
    priorityoffset = models.CharField(max_length=180, db_column='priorityoffset', blank=True)
    allowedgroups = models.CharField(max_length=300, db_column='allowedgroups', blank=True)
    defaulttoken = models.CharField(max_length=300, db_column='defaulttoken', blank=True)
    pcache = models.CharField(max_length=300, db_column='pcache', blank=True)
    validatedreleases = models.CharField(max_length=1500, db_column='validatedreleases', blank=True)
    accesscontrol = models.CharField(max_length=60, db_column='accesscontrol', blank=True)
    dn = models.CharField(max_length=300, db_column='dn', blank=True)
    email = models.CharField(max_length=180, db_column='email', blank=True)
    allowednode = models.CharField(max_length=240, db_column='allowednode', blank=True)
    maxinputsize = models.IntegerField(null=True, db_column='maxinputsize', blank=True)
    timefloor = models.IntegerField(null=True, db_column='timefloor', blank=True)
    depthboost = models.IntegerField(null=True, db_column='depthboost', blank=True)
    idlepilotsupression = models.IntegerField(null=True, db_column='idlepilotsupression', blank=True)
    pilotlimit = models.IntegerField(null=True, db_column='pilotlimit', blank=True)
    transferringlimit = models.IntegerField(null=True, db_column='transferringlimit', blank=True)
    cachedse = models.IntegerField(null=True, db_column='cachedse', blank=True)
    corecount = models.IntegerField(null=True, db_column='corecount', blank=True)
    countrygroup = models.CharField(max_length=192, db_column='countrygroup', blank=True)
    availablecpu = models.CharField(max_length=192, db_column='availablecpu', blank=True)
    availablestorage = models.CharField(max_length=192, db_column='availablestorage', blank=True)
    pledgedcpu = models.CharField(max_length=192, db_column='pledgedcpu', blank=True)
    pledgedstorage = models.CharField(max_length=192, db_column='pledgedstorage', blank=True)
    statusoverride = models.CharField(max_length=768, db_column='statusoverride', blank=True)
    allowdirectaccess = models.CharField(max_length=30, db_column='allowdirectaccess', blank=True)
    gocname = models.CharField(max_length=192, db_column='gocname', blank=True)
    tier = models.CharField(max_length=45, db_column='tier', blank=True)
    multicloud = models.CharField(max_length=192, db_column='multicloud', blank=True)
    lfcregister = models.CharField(max_length=30, db_column='lfcregister', blank=True)
    stageinretry = models.IntegerField(null=True, db_column='stageinretry', blank=True)
    stageoutretry = models.IntegerField(null=True, db_column='stageoutretry', blank=True)
    fairsharepolicy = models.CharField(max_length=1536, db_column='fairsharepolicy', blank=True)
    allowfax = models.CharField(null=True, max_length=64, db_column='allowfax', blank=True)
    faxredirector = models.CharField(null=True, max_length=256, db_column='faxredirector', blank=True)
    maxwdir = models.IntegerField(null=True, db_column='maxwdir', blank=True)
    celist = models.CharField(max_length=12000, db_column='celist', blank=True)
    minmemory = models.IntegerField(null=True, db_column='minmemory', blank=True)
    maxmemory = models.IntegerField(null=True, db_column='maxmemory', blank=True)
    minrss = models.IntegerField(null=True, db_column='minrss', blank=True)
    maxrss = models.IntegerField(null=True, db_column='maxrss', blank=True)
    mintime = models.IntegerField(null=True, db_column='mintime', blank=True)
    allowjem = models.CharField(null=True, max_length=64, db_column='allowjem', blank=True)
    catchall = models.CharField(null=True, max_length=512, db_column='catchall', blank=True)
    faxdoor = models.CharField(null=True, max_length=128, db_column='faxdoor', blank=True)
    wansourcelimit = models.IntegerField(null=True, db_column='wansourcelimit', blank=True)
    wansinklimit = models.IntegerField(null=True, db_column='wansinklimit', blank=True)
    auto_mcu = models.SmallIntegerField(null=True, db_column='auto_mcu', blank=True)
    objectstore = models.CharField(null=True, max_length=512, db_column='objectstore', blank=True)
    allowhttp = models.CharField(null=True, max_length=64, db_column='allowhttp', blank=True)
    httpredirector = models.CharField(null=True, max_length=256, db_column='httpredirector', blank=True)
    multicloud_append = models.CharField(null=True, max_length=64, db_column='multicloud_append', blank=True)
    corepower = models.IntegerField(null=True, db_column='corepower', blank=True)
    #Were added 21.12.17
    directaccesslan = models.CharField(null=True, max_length=64, db_column='direct_access_lan', blank=True)
    directaccesswan = models.CharField(null=True, max_length=64, db_column='direct_access_wan', blank=True)
    wnconnectivy = models.CharField(null=True, max_length=256, db_column='wnconnectivity', blank=True)
    cloudrshare = models.CharField(null=True, max_length=256, db_column='cloudrshare',  blank=True)
    sitershare = models.CharField(null=True, max_length=256, db_column='sitershare',  blank=True)
    autosetup_post = models.CharField(null=True, max_length=512, db_column='autosetup_post', blank=True)
    autosetup_pre = models.CharField(null=True, max_length=512, db_column='autosetup_pre', blank=True)
    use_newmover = models.CharField(null=True, max_length=32, db_column='use_newmover', blank=True)
    pilotversion = models.CharField(null=True, max_length=32, db_column='pilotversion', blank=True)
    objectstores = models.CharField(null=True, max_length=4000, db_column='objectstores', blank=True)
    container_options = models.CharField(null=True, max_length=1024, db_column='container_options',blank=True)
    container_type = models.CharField(null=True, max_length=256, db_column='container_type', blank=True)
    jobseed = models.CharField(null=True, max_length=16, db_column='jobseed', blank=True)
    pilot_manager = models.CharField(null=True, max_length=16, db_column='pilot_manager', blank=True)


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
        db_table = f'"{settings.DB_SCHEMA_PANDA_META}"."schedconfig"'
        app_label = 'panda'


class SchedconfigJson(models.Model):
    pandaqueue = models.CharField(max_length=180, db_column='panda_queue', primary_key=True)
    data = models.TextField(db_column='data', blank=True)
    lastupdate = models.DateField(db_column='last_update')

    class Meta:
        db_table = f'"{settings.DB_SCHEMA_PANDA}"."schedconfig_json"'
        app_label = 'panda'
