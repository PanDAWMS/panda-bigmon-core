import getopt, subprocess, re, cx_Oracle, requests, json, psutil

import sys
from datetime import datetime
from configparser import ConfigParser
from logger import ServiceLogger

_logger = ServiceLogger("servicemonitoring", __file__).logger

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()


def cpu_info():
    cpu_times = psutil.cpu_times()
    cpu_usage_list = []
    for x in range(5):
        cpu_usage_list.append(psutil.cpu_percent(interval=2, percpu=True))
    return cpu_times, cpu_usage_list


def memory_info():
    memory_virtual = psutil.virtual_memory()
    memory_swap = psutil.swap_memory()
    return memory_virtual, memory_swap


def disk_info(disk=''):
    if disk == '':
        full_path = '/'
    else:
        full_path = '/' + disk
    disk_usage = psutil.disk_usage(full_path)
    return disk_usage


def make_db_connection(cfg):
    try:
        dbuser = cfg.get('pandadb', 'login')
        dbpasswd = cfg.get('pandadb', 'password')
        description = cfg.get('pandadb', 'description')
    except:
        _logger.error('Settings for Oracle connection not found')
        return None
    try:
        connection = cx_Oracle.connect(dbuser, dbpasswd, description)
        _logger.debug('DB connection established. "{0}" "{1}"'.format(dbuser, description))
        return connection
    except Exception as ex:
        _logger.error(ex)
        return None


def logstash_configs(cfg):
    try:
        url = cfg.get('logstash', 'url')
        port = cfg.get('logstash', 'port')
        auth = [x.strip() for x in cfg.get('logstash', 'auth').split(',')]
        auth = (auth[0], auth[1])
        _logger.debug('Logstash settings have been read. "{0}" "{1}" "{2}"'.format(url, port, auth))
        return url, port, auth
    except:
        _logger.error('Settings for logstash not found')
        return None, None, None


def monit_sls_configs(cfg):
    if cfg.has_section('monitsls'):
        try:
            host = cfg.get('monitsls', 'host')
            port = cfg.get('monitsls', 'port')
            service_name =  cfg.get('monitsls', 'service')
            _logger.debug(
                f'Monit SLS settings have been read. URL: {host}:{port}. Service: {service_name}')
            return host, port, service_name
        except:
            _logger.error('Settings for Monit SLS not found')
            return None, None, None
    else:
        _logger.warn('No section with Monit SLS settings in the .ini file')
        return None, None, None


def servers_configs(cfg):
    metrics = None
    if cfg.has_section('othersettings') and cfg.has_option('othersettings', 'metrics'):
        metrics = {x.strip(): [] for x in cfg.get('othersettings', 'metrics').split(',')}

        for m in metrics:
            if cfg.has_option('othersettings', m):
                metrics[m].extend([x.strip() for x in cfg.get('othersettings', m).split(',')])
        _logger.debug(f'Server settings have been read: {metrics}')
    else:
        _logger.error('Settings for servers configs not found')
    return metrics


def volume_use(volume_name):
    command = "df -Pkh /" + volume_name
    used_amount = 0
    tmp_array = command.split()
    try:
        output = subprocess.Popen(tmp_array, stdout=subprocess.PIPE).communicate()[0].decode("utf-8")
    except:
        return None
    for line in output.split('\n'):
        if re.search(volume_name, line):
            used_amount = re.search(r"(\d+)\%", line).group(1)
    if used_amount == 0:
        _logger.debug('df: "{0}": No such file or directory'.format(volume_name))
    try:
        used_amount_float = float(used_amount)
    except ValueError:
        used_amount_float = None

    return used_amount_float


def process_availability_psutil(process_name):
    availability = '0'
    avail_info = '{0} process not found'.format(process_name)
    for proc in psutil.process_iter():
        process = psutil.Process(proc.pid)  # Get the process info using PID
        pname = process.name()  # Here is the process name
        if pname == process_name:
            availability = '100'
            avail_info = '{0} running'.format(process_name)

    return availability, avail_info


def process_availability(process_name):
    availability = '0'
    avail_info = '{0} process not found'.format(process_name)

    output = subprocess.Popen("ps -eo pgid,args | grep {0} | grep -v grep | uniq".format(process_name),
                              stdout=subprocess.PIPE, shell=True).communicate()[0]
    if str(output) != "b''":
        availability = '100'
        avail_info = '{0} running'.format(process_name)
    return availability, avail_info


def subprocess_availability(subprocess_name):
    availability = '0'
    avail_info = '{0} process not found'.format(subprocess_name)
    try:
        output = subprocess.check_output([subprocess_name, "status"], stderr=subprocess.STDOUT)
        if str(output) != "b''":
            subprocess_info = output.decode('utf-8')
            subprocess_info = subprocess_info.split()
            if subprocess_info[1] == "RUNNING":
                availability = '100'
                avail_info = '{0} running'.format(subprocess_name)
            else:
                availability = '0'
                avail_info = '{0} stopped'.format(subprocess_name)
    except:
        pass
    return availability, avail_info


def send_data(data, settings):
    url, port, auth = logstash_configs(settings)
    try:
        code = requests.post(
            url='http://{0}:{1}'.format(url, port),
            data=data,
            auth=auth)
        if code.status_code == 200:
            _logger.debug('Status code: {0}'.format(code.status_code))
        else:
            _logger.error('Status code: {0}'.format(code.status_code))
    except Exception as ex:
        _logger.debug('Data can not be sent. {0}'.format(ex))


def get_settings_path(argv):
    cfg = ConfigParser()
    path, type = '', ''
    try:
        opts, args = getopt.getopt(argv, "hi:s:t:", ["settings=", "type="])
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-s' or opt == '-settings':
            path = str(arg)
        if opt == '-t' or opt == '-type':
            type = str(arg)
    if path == '':
        path = 'cron_settings.ini'
    if type == '':
        type = None
    cfg.read(path)
    if cfg.has_section('logstash') and cfg.has_section('logstash') and cfg.has_section('logstash'):
        return cfg, type
    else:
        _logger.error('Settings file not found. {0}'.format(path))
    return None, None

