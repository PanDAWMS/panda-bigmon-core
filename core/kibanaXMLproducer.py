#Ideas and pieces of code taken from here: https://svnweb.cern.ch/trac/adcsw/browser/adcmon/service_monitoring/trunk/SLSmon-XML-producers/PanDA/panda_server-makeKibanaXml.py

__author__ = 'spadolski'

import subprocess
import optparse
#import kibanaXML
import re
import socket
from xml.dom import minidom
import time
import datetime

serverToPost = 'xsls.cern.ch'
patternOfXMLFileName = 'bigpandamon_%s'

parser = optparse.OptionParser()
parser.add_option( "--host", dest="host", type="string",
    help="Hostname of server to check, default is current machine hostname" )
parser.add_option( "-d", "--dir", dest="dir", type="string",
        help="Filename of the xml file output.  Default is /tmp/" )

( options, args ) = parser.parse_args()

def post_xml(file_name, monitor):
    subprocess.call(["curl", "-F", "file=@%s"%file_name, monitor])

def httpd_availability( host ) :
    url = 'http://%s.cern.ch/robots.txt' % ( host )
    return check_url( url, "go away" )


def check_url( url, check_string ) :
    command = "wget -q -O - " + url
    return check_command( command, check_string )


def check_command( command, check_string ) :
    if( options.debug ) :
        print "Checking command : %s" % ( command )
        print "For string : %s" % ( check_string )

    tmp_array = command.split()
    output = subprocess.Popen( tmp_array, stdout=subprocess.PIPE ).communicate()[0]

    if( re.search( check_string, output ) ) :
        if( options.debug ) : print "Found the string, return 100"
        return '100'
    else:
        if( options.debug ) : print "String not found, return 0"
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



def make_monitor( host ) :
    if( options.debug ) : print "Creating the monitor monitoring xml"
    errormes = False
    messagetext = ''
    http_avail = httpd_availability( host )
    if( http_avail == 0 ) :
        errormes = True
        messagetext += "Error: web server on %s not working\n" % ( host )

    panda_avail = panda_availability( host )
    if( panda_avail == 0 ) :
         errormes = True
         messagetext += "Error: panda monitor on %s not working\n" % ( host )

    http_processes = count_processes()
    data_used = volume_use( 'data' )
    var_used = volume_use( 'var' )
    if( options.debug ) :
        print 'web - %s, squid - %s, panda - %s' % ( http_avail, squid_avail, panda_avail )
    kibana_xml = kibanaXML.xml_doc()
    kibana_xml.set_id( 'PandaMon_%s' % ( host ) )
    kibana_xml.set_availability( str( panda_avail ) )
    kibana_xml.add_data( "HttpdAvailability", "Availability of the httpd server", str( http_avail ) )
    kibana_xml.add_data( "PandaAvailability", "Availability of the panda monitor", str( panda_avail ) )
    kibana_xml.add_data( "HttpProcesses", "Number of processes for the panda monitor", str( http_processes ) )
    kibana_xml.add_data( "DataVolumeUse", "Percent use of the local /data volume", str( data_used ) )
    kibana_xml.add_data( "VarVolumeUse", "Percent use of the local /var volume", str( var_used ) )
    return kibana_xml.print_xml()

def generateXML(hostId):
    doc = minidom.Document()

    serviceupdate = doc.createElement('serviceupdate')
    serviceupdate.setAttribute("xmlns", "http://sls.cern.ch/SLS/XML/update")
    doc.appendChild(serviceupdate)
    id = doc.createElement('id')
    text = doc.createTextNode(hostId)
    id.appendChild(text)

    timeformat = '%Y-%m-%dT%H:%M:%S'
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime(timeformat)

    timestampNode = doc.createElement('timestamp')
    timestampValue = doc.createTextNode(st)
    timestampNode.appendChild(timestampValue)

    statusNode = doc.createElement('status')
    # available, degraded 0.2 or unavailable
    statusValue = doc.createTextNode('available')
    statusNode.appendChild(statusValue)

    dataNode = doc.createElement('data')

    data_used = volume_use( 'data' )
    var_used  = volume_use( 'var' )
    http_processes = count_processes()


    numericvalueNode = doc.createElement('numericvalue')
    numericvalueNode.setAttribute('name', "DataVolumeUse")
    numericvalueNodeValue = doc.createTextNode(data_used)
    numericvalueNode.appendChild(numericvalueNodeValue)
    dataNode.appendChild(numericvalueNode)

    numericvalueNode = doc.createElement('numericvalue')
    numericvalueNode.setAttribute('name', "VarVolumeUse")
    numericvalueNodeValue = doc.createTextNode(var_used)
    numericvalueNode.appendChild(numericvalueNodeValue)
    dataNode.appendChild(numericvalueNode)

    numericvalueNode = doc.createElement('numericvalue')
    numericvalueNode.setAttribute('name', "HttpProcesses")
    numericvalueNodeValue = doc.createTextNode(http_processes.__str__())
    numericvalueNode.appendChild(numericvalueNodeValue)
    dataNode.appendChild(numericvalueNode)

    serviceupdate.appendChild(id)
    serviceupdate.appendChild(timestampNode)
    serviceupdate.appendChild(statusNode)
    serviceupdate.appendChild(dataNode)

    xml_str = doc.toprettyxml(encoding="utf-8", newl='')

#    with open("minidom_example.xml", "w") as f:
#        f.write(xml_str)
    return xml_str


def __main__() :

    if (options.host):
        host = options.host
    else :
        host = socket.gethostname()
        host = re.sub( r'^(\w+).*', r'\1', host )

    if( options.dir ) :
        file_dir = options.dir
    else :
        file_dir = '/tmp'

    hostId = patternOfXMLFileName % host
    print generateXML(hostId)



#run program
__main__()