import getopt, subprocess, re, requests, json, psutil

import sys
from datetime import datetime
from configparser import ConfigParser
from logger import ServiceLogger

_logger = ServiceLogger("servicemonitoring", __file__).logger

try:
    import oracledb
    oracledb.init_oracle_client(config_dir='/etc/tnsnames.ora')
except oracledb.exceptions.DatabaseError as e:
    _logger.error(f"Failed to initialize Oracle Client: {e}")
except Exception as e:
    _logger.error(f"An unexpected error occurred: {e}")

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
        from core import settings
        dbaccess = settings.local.dbaccess['default']
    except:
        dbaccess = None
        _logger.info("Failed to get credentials for DB connection from bigpanda settings")

    if dbaccess:
        db_user = dbaccess['USER']
        db_passwd = dbaccess['PASSWORD']
        db_description = f"""(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=adcr-s.cern.ch)(PORT=10121))(LOAD_BALANCE=on)
            (ENABLE=BROKEN)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME={dbaccess['NAME']}.cern.ch)))"""
    elif cfg.has_section('pandadb') and all([cfg.has_option('pandadb', x) for x in ('login', 'password', 'description')]):
        db_user = cfg.get('pandadb', 'login')
        db_passwd = cfg.get('pandadb', 'password')
        db_description = cfg.get('pandadb', 'description')
    else:
        db_user, db_passwd, db_description = None, None, None
        _logger.error('Settings for Oracle connection not found')

    if db_user and db_passwd and db_description:
        try:
            connection = oracledb.connect(user=db_user, password=db_passwd, dsn=db_description)
            _logger.debug('DB connection established. "{0}" "{1}"'.format(db_user, db_description))
            return connection
        except Exception as ex:
            _logger.error(ex)

    return None



def db_sessions(connection, hostname='all'):
    """
    Get DB sessions info using special ATLAS_DBA view
    :return: N active sessions, N total sessions
    """
    n_active_sessions = None
    n_sessions = None

    where_clause = ''
    if hostname != 'all':
        where_clause = f" where machine like '%%{hostname}%%'"
    query = f"select sum(num_active_sess), sum(num_sess) from atlas_dba.count_pandamon_sessions {where_clause}"

    cursor = connection.cursor()
    try:
        cursor.execute(query)
        for row in cursor:
            n_active_sessions = row[0]
            n_sessions = row[1]
            break
    except Exception as ex:
        _logger.exception(f"Failed to execute query with {ex}")
    cursor.close()
    _logger.info(f"Got sessions counts for {hostname} host(s): active={n_active_sessions}, total={n_sessions}")
    return n_active_sessions, n_sessions


def db_cache_entries(connection):
    """
    Get number of cache entries in djangocache table
    :return: N cache entries
    """
    n_cache_entries = None
    query = f"select count(cache_key) from atlas_pandabigmon.djangocache"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        for row in cursor:
            n_cache_entries = row[0]
            break
    except Exception as ex:
        _logger.exception(f"Failed to execute query with {ex}")
    cursor.close()
    _logger.info(f"Got number of cache entries, total={n_cache_entries}")
    return n_cache_entries


def is_any_requests_lately(connection, hostname="all", n_last_minutes=10):
    """
    Check if any requests  came to nodes
    :param connection: cx_oracle connection
    :param hostname: str
    :param n_last_minutes: int
    :return:
    """
    n_requests, duration_median = None, None
    where_clause = f" where qtime > CAST(sys_extract_utc(SYSTIMESTAMP) AS DATE) - interval '{n_last_minutes}' minute "
    if hostname != 'all':
        where_clause += f" and server like '%%{hostname}%%'"
    query = f"""
        select count(id) as n, 
            median(extract(minute from (rtime-qtime))*60 + extract(second from (rtime-qtime))) as duration_median 
        from atlas_pandabigmon.all_requests_daily {where_clause}
    """
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        for row in cursor:
            n_requests = row[0]
            duration_median = row[1]
            break
    except Exception as ex:
        _logger.exception(f"Failed to execute query \n{query} \nwith {ex}")
    cursor.close()
    _logger.info(f"Got N requests for {hostname} host(s): n={n_requests}, median TTR={n_requests}")
    return n_requests, duration_median


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


def service_availability(metrics:dict) -> tuple[str, str]:
    """
    Check metrics and return service status as a number from 0 to 100 as str and its description
    :param metrics: dict - previously collected metrics
    :return: availability: str - a number from 0 to 100
    :return: availability_desc: str - description
    """
    process = 'httpd'
    threshold_ttr = 10

    if process in metrics:
        availability = int(metrics[process])
        availability_desc = metrics[process + '_info']
    else:
        availability = 0
        availability_desc = f"{process} process not found"
        return str(availability), availability_desc

    if 'requests_last_10min_count' in metrics and metrics['requests_last_10min_count'] is not None:
        if metrics['requests_last_10min_count'] == 0:
            availability = 0
            availability_desc += ', no requests processed in last 10 minutes'
            return str(availability), availability_desc
        elif metrics['requests_last_10min_count'] <= 10:
            availability -= 50
            availability_desc += ', low number of requests processed lately'
    if 'requests_last_10min_duration_median' in metrics and metrics['requests_last_10min_duration_median'] is not None:
        if metrics['requests_last_10min_duration_median'] > threshold_ttr:
            availability -= 20
            availability_desc += f', median TTR is more than {threshold_ttr} sec'

    return str(availability), availability_desc


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

