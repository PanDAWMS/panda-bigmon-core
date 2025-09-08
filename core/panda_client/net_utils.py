import copy
import os
import random
import socket

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.connection import allowed_gai_family

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from .thread_utils import MapWithLockAndTimeout

# DNS cache
dnsMap = MapWithLockAndTimeout()
print(dnsMap)

# HTTP adaptor with randomized DNS resolution
class HTTPAdapterWithRandomDnsResolver(HTTPAdapter):
    # override to get connection to random host
    def get_connection(self, url, proxies=None):
        # resolve cname to hostnames
        dns_records = resolve_host_in_url(url)
        random.shuffle(dns_records)
        # parse URL
        parsed = urlparse(url)
        # loop over all hosts
        err = None
        for hostname in dns_records:
            tmp_url = replace_hostname_in_url(url, hostname)
            try:
                con = HTTPAdapter.get_connection(self, tmp_url, proxies=proxies)
                # return if valid
                if con is not None:
                    return con
            except Exception as e:
                err = e
        if err is not None:
            raise err
        return None


# utility function to get a session object with HTTPAdapterWithRandomDnsResolver
def get_http_adapter_with_random_dns_resolution() -> requests.Session:
    """
    Utility function to get a session object with custom HTTPAdapter which resolves host in URL randomly to distribute access to DNS load balanced HTTP services
    :return: session object
    """
    session = requests.Session()
    # no randomization if panda is behind real load balancer than DNS LB
    if "PANDA_BEHIND_REAL_LB" in os.environ:
        return session
    adapter = HTTPAdapterWithRandomDnsResolver(max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# resolve a host in a given URL to hostnames
def resolve_host_in_url(url: str) -> list[str]:
    """
    Resolve a host in a given URL to hostnames
    :param url: URL
    :return: list of hostnames
    """
    # parse URL
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port
    if port is None:
        if parsed.scheme == "http":
            port = 80
        else:
            port = 443
    # check record
    if parsed.hostname in dnsMap:
        dns_records = dnsMap[parsed.hostname]
    else:
        family = allowed_gai_family()
        dns_records = socket.getaddrinfo(host, port, family, socket.SOCK_STREAM)
        dns_records = list(set([socket.getfqdn(record[4][0]) for record in dns_records]))
        dnsMap[parsed.hostname] = dns_records
    return copy.copy(dns_records)


# replace hostname in URL
def replace_hostname_in_url(url: str, new_host: str) -> str:
    """
    Replace hostname in URL
    :param url: original URL
    :param new_host: new hostname
    :return: new URL with replaced hostname
    """
    parsed = urlparse(url)
    if parsed.port is not None:
        new_host += f":{parsed.port}"
    return parsed._replace(netloc=new_host).geturl()


# replace hostname in URL randomly
def replace_hostname_in_url_randomly(url: str) -> str:
    """
    Replace hostname in URL randomly
    :param url: original URL
    :return: new URL with new hostname randomly chosen from resolved hostnames
    """
    # no replacement if panda is behind real load balancer than DNS LB
    if "PANDA_BEHIND_REAL_LB" in os.environ:
        return url
    # resolve cname to hostnames
    dns_records = resolve_host_in_url(url)
    # choose one IP randomly
    random.shuffle(dns_records)
    return replace_hostname_in_url(url, dns_records[0])
