"""
Cron to submit service metrics to Logstash & MONIT SLS
"""
import sys
import json
import socket
import numpy as np
from datetime import datetime, timezone

from utils import get_settings_path, servers_configs, monit_sls_configs, cpu_info, memory_info, disk_info, volume_use, \
    process_availability, subprocess_availability, DateTimeEncoder, send_data, service_availability, \
    make_db_connection, db_sessions, db_cache_entries, is_any_requests_lately
from sls_document import SlsDocument
from logger import ServiceLogger

_logger = ServiceLogger("servicemonitoring", __file__).logger


def main():
    dict_metrics = {}
    settings_path, service_type = get_settings_path(sys.argv[1:])

    if settings_path is not None:
        hostname = socket.gethostname()
        dict_metrics['hostname'] = hostname
        metrics = servers_configs(settings_path)

        if metrics and 'cpu' in metrics:
            cpu_times, cpu_usage = cpu_info()
            dict_metrics['avg_cpu_usage'] = np.array(cpu_usage).mean() / 100

        if metrics and 'memory' in metrics:
            memory_virtual, memory_swap = memory_info()
            dict_metrics['memory_active'] = memory_virtual.active
            dict_metrics['memory_available'] = memory_virtual.available
            dict_metrics['memory_buffers'] = memory_virtual.buffers
            dict_metrics['memory_cached'] = memory_virtual.cached
            dict_metrics['memory_free'] = memory_virtual.free
            dict_metrics['memory_shared'] = memory_virtual.shared
            dict_metrics['memory_slab'] = memory_virtual.slab
            dict_metrics['memory_total'] = memory_virtual.total
            dict_metrics['memory_used'] = memory_virtual.used
            dict_metrics['memory_usage_pc'] = memory_virtual.percent / 100
            dict_metrics['memory_free_pc'] = memory_virtual.available / memory_virtual.total

            dict_metrics['swap_free'] = memory_swap.free
            dict_metrics['swap_sin'] = memory_swap.sin
            dict_metrics['swap_sout'] = memory_swap.sout
            dict_metrics['swap_total'] = memory_swap.total
            dict_metrics['swap_used'] = memory_swap.used
            dict_metrics['swap_usage_pc'] = memory_swap.percent / 100
            try:
                dict_metrics['swap_free_pc'] = (memory_swap.free / memory_swap.total)
            except:
                dict_metrics['swap_free_pc'] = 0

        if metrics and 'disk' in metrics:
            for diskname in metrics['disk']:
                disk = disk_info(diskname)
                dict_metrics[diskname + '_total'] = disk.total
                dict_metrics[diskname + '_used'] = disk.used
                dict_metrics[diskname + '_free'] = disk.free
                dict_metrics[diskname + '_usage_pc'] = disk.percent / 100
                dict_metrics[diskname + '_free_pc'] = (disk.free / disk.total)

        if metrics and 'volume' in metrics:
            for vol_name in metrics['volume']:
                try:
                    volume = volume_use(vol_name)
                    dict_metrics[vol_name+'_used_pc'] = volume
                except Exception as ex:
                    _logger.error(ex)

        if metrics and 'process' in metrics:
            for process in metrics['process']:
                try:
                    proc_avail, proc_avail_info = process_availability(process)
                    dict_metrics[process] = proc_avail
                    dict_metrics[process + '_info'] = proc_avail_info
                except Exception as ex:
                    _logger.error(ex)

        if metrics and 'subprocess' in metrics:
            for subprocess in metrics['subprocess']:
                try:
                    proc_avail, proc_avail_info = subprocess_availability(subprocess)
                    dict_metrics[subprocess] = proc_avail
                    dict_metrics[subprocess + '_info'] = proc_avail_info
                except Exception as ex:
                    _logger.error(ex)

        if metrics and ('dbsession' in metrics or 'dbcache' in metrics or 'dbrequests' in metrics):
            conn = None
            try:
                conn = make_db_connection(settings_path)
            except Exception as ex:
                _logger.error(ex)

            if conn:
                if 'dbsession' in metrics:
                    n_active_sessions, n_sessions = db_sessions(connection=conn, hostname=hostname)
                    if n_active_sessions is not None and n_sessions is not None:
                        dict_metrics['db_n_active_sessions'] = n_active_sessions
                        dict_metrics['db_n_sessions'] = n_sessions
                if 'dbrequests' in metrics:
                    n_requests_last_n_minutes, request_duration_median = is_any_requests_lately(
                        connection=conn, hostname=hostname, n_last_minutes=10)
                    if n_requests_last_n_minutes is not None:
                        dict_metrics['requests_last_10min_count'] = n_requests_last_n_minutes
                        dict_metrics['requests_last_10min_duration_median'] = request_duration_median
                if 'dbcache' in metrics:
                    n_cache_entries = db_cache_entries(connection=conn)
                    if n_cache_entries is not None:
                        dict_metrics['n_cache_entries'] = n_cache_entries
                conn.close()

        # send metrics to logstash
        dict_metrics['creation_time'] = datetime.now(tz=timezone.utc)
        if service_type is not None:
            dict_metrics['monittype'] = service_type
            send_data(json.dumps(dict_metrics, cls=DateTimeEncoder), settings_path)
            _logger.info("data has been sent {0}".format(json.dumps(dict_metrics,cls=DateTimeEncoder)))
        else:
            _logger.error("Type is not defined")

        # send document to SLS
        monit_host, monit_port, service_name = monit_sls_configs(settings_path)
        if monit_host and monit_port and service_name:
            sls_doc = SlsDocument()
            sls_doc.set_id(f'{service_name}_{hostname}')
            sls_doc.set_avail_info(service_name)

            status, desc = service_availability(dict_metrics)
            sls_doc.set_status(status)
            sls_doc.set_avail_desc(desc)

            sls_doc.send_document(collector_endpoint=f'http://{monit_host}:{monit_port}')
        else:
            _logger.error("No Monit SLS settings found, can not send anything")


if __name__ == '__main__':
    main()