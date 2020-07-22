
#Ideas and several pieces of code taken from here: https://svnweb.cern.ch/trac/adcsw/browser/adcmon/service_monitoring/trunk/SLSmon-XML-producers/PanDA/panda_server-makeKibanaXml.py

__author__ = 'spadolski'

import subprocess
import optparse
import re
import socket
import time
import datetime
import shlex
from lxml import etree
import requests

serverToPost = 'http://xsls.cern.ch'
patternOfXMLFileName = 'bigpandamon_%s'

parser = optparse.OptionParser()
parser.add_option( "--host", dest="host", type="string",
    help="Hostname of server to check, default is current machine hostname" )

parser.add_option( "--redis", dest="redis", type="string",
    help="Hostname of redis cache server")


( options, args ) = parser.parse_args()


def httpd_availability( host ) :
    url = 'http://%s.cern.ch/robots.txt' % ( host )
    return check_url( url, "go away" )

def redis_availability(redisAddress):
    proc1 = subprocess.Popen(shlex.split(" redis-benchmark -h " + redisAddress + " -c 10 -n 1000 -q ") , stdout=subprocess.PIPE )
    proc2 = subprocess.Popen(shlex.split('grep GET'), stdin=proc1.stdout, stdout=subprocess.PIPE)
    proc1.stdout.close()
    output=proc2.communicate()[0]
    output = re.findall("[-+]?\d+[\.]?\d*", output)
    if len(output) > 0:
        avail = int(float(output[0])*100/25000)
    else:
        avail = 0

    if avail > 100: avail  = 100
    return avail


def check_url( url, check_string ) :
    command = "wget -q -O - " + url
    return check_command( command, check_string )


def check_command( command, check_string ) :
    tmp_array = command.split()
    output = subprocess.Popen( tmp_array, stdout=subprocess.PIPE ).communicate()[0]

    if( re.search( check_string, output ) ) :
        return '100'
    else:
        return '0'


def count_processes() :
    output = subprocess.Popen( ['ps', 'aux'], stdout=subprocess.PIPE ).communicate()[0]
    count = 0
    for line in output.split( '\n' ) :
#        if( re.match( 'atlpan', line ) ) :
         if( re.search( 'http', line ) ) :
            count += 1
    return count

def volume_use( volume_name ) :
    command = "df -Pkh /" + volume_name
    used_amount = 0
    tmp_array = command.split()
    output = subprocess.Popen( tmp_array, stdout=subprocess.PIPE ).communicate()[0]

    for line in output.split( '\n' ) :
        if( re.search( volume_name, line ) ) :
            used_amount = re.search( r"(\d+)\%", line ).group(1)
    return used_amount


def generateXML(hostId, redisAddress):

    root = etree.Element("serviceupdate", xmlns="http://sls.cern.ch/SLS/XML/update")
    etree.SubElement(root, "id").text = hostId

    dataroot = etree.Element("data")
    data_used = volume_use( 'data' )
    var_used  = volume_use( 'var' )
    http_processes = count_processes()
    http_avail = httpd_availability( hostId )
    redis_avail = redis_availability(redisAddress)

    status = 'available'
    if( http_avail == 0 ) :
        status = 'unavailable'



    timeformat = '%Y-%m-%dT%H:%M:%S'
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime(timeformat)
    etree.SubElement(root, "timestamp").text = st
    etree.SubElement(root, "status").text = status


    valElement = etree.SubElement(dataroot, "numericvalue")
    valElement.set('name','DataVolumeUse')
    valElement.text = data_used

    valElement = etree.SubElement(dataroot, "numericvalue")
    valElement.set('name','VarVolumeUse')
    valElement.text = var_used

    valElement = etree.SubElement(dataroot, "numericvalue")
    valElement.set('name','HttpProcesses')
    valElement.text = str(http_processes)

    valElement = etree.SubElement(dataroot, "numericvalue")
    valElement.set('name','RedisAvail')
    valElement.text = str(redis_avail)


    root.append(dataroot)
    xml_str = etree.tostring(root, pretty_print=True, encoding='utf-8', xml_declaration=True)
    return xml_str


def __main__() :

    if (options.host):
        host = options.host
    else :
        host = socket.gethostname()
        host = re.sub( r'^(\w+).*', r'\1', host )

    if (options.redis):
        redisAddress = options.redis
    else:
        redisAddress = ''

    hostId = patternOfXMLFileName % host
    xmlFile = generateXML(hostId, redisAddress)
    result = ''
    try:
        result = requests.post(serverToPost, files=dict(file=(xmlFile)))
    finally:
        print (result)

__main__()