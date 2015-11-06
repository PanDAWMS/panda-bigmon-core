PanDA status summary API
=====

PanDA status summary is a simple Django app to show summary of number jobs per job status for each active PanDA resource. Active PanDA resource is every resource which ran jobs in the past N hours (default is N=12 hours).


API: defaults, available filters and their combinations
-----------

The API listens to following GET parameters as job properties (from jobs tables):
* ?nhours=N or ?starttime=XYZ&endtime=UVW
* ?mcp_cloud=XYZ
* ?computingsite=XYZ
* ?jobstatus=XYZ

The API listens to following GET parameters as PanDA resource properties (from schedconfig table):
* ?corecount=N
* ?cloud=XYZ
* ?atlas_site=XYZ
* ?status=XYZ

Default job 'modificationtime' range is nhours=12 (last 12 hours). API filters listed earlier can be combined. Default URL /status_summary/ shows all active jobs from the past 12 hours, there is no distinction between analysis, production, test etc. jobs. 

Filters 'mcp_cloud', 'computingsite', 'jobstatus', 'cloud', 'atlas_site', 'status' listen to wildcard '*'. 

Operator != can be expressed with '-', e.g. 'field != value' translates into 'field=-value'. 

'NULL' values from DB can also be filtered, e.g. 'field == NULL' translates into '?field=NULL', 'field != NULL' translates into '?field=-NULL'.

Filters are described in more details in the following sections. 


API: ?nhours=N or ?starttime=XYZ&endtime=UVW
-----------
The 'nhours' parameter defines activity interval of "last N hours", format: an integer. It can be alternated by pair of 'starttime' and 'endtime' parameters, format: %Y-%m-%dT%H:%M:%S.

The 'nhours' parameter has higher priority than 'starttime', 'endtime' parameters: if 'nhours' is specified, 'starttime' and 'endtime' are not taken into account.

The API has 3 HTTP return states: 200, 404, 400.

**200 OK**: job records satisfying the GET parameters were found in DB, no errors while querying.

**404 NOT FOUND**: query parameters were provided, but no job satisfies the query parametersExample of returned error message:
  ```
'errors': {'lookup': 'Job for this query has not been found. '}
  ```
**400 BAD REQUEST**:

* Catch all for all other errors.


In any case, the data dictionary with the following keys is returned in the response: 
  ```
'GET_parameters'  ... dictionary of request.GET,
'query'        ... QuerySet filter parameter used to retrieve PanDA job records, 
'nrecords'        ... integer specifies how many PanDA logger records have been found, 
'data'       ... list of PanDA logger records found, 
'timestamp'  ... datetime in isoformat, time when the response has been sent,
'errors'     ... dictionary with list of errors encountered, 
'warnings'   ... dictionary with list of warnings encountered, e.g. when an optional parameter is missing.
'errors'     ... dictionary with list of errors encountered, 
  ```


**Example usage**:

Successful pass:
  ```
# curl -v -H 'Accept: application/json' -H 'Content-Type: application/json' \
  "http://HOSTNAME/status_summary/?nhours=2"
* About to connect() to HOSTNAME port 80 (#0)
*   Trying IP-ADDRESS... connected
> GET /status_summary/?nhours=2 HTTP/1.1
> User-Agent: curl/7.22.0 (x86_64-pc-linux-gnu) libcurl/7.22.0 OpenSSL/1.0.1 zlib/1.2.3.4 libidn/1.23 librtmp/2.3
> Host: HOSTNAME
> Accept: application/json
> Content-Type: application/json
> 
  0     0    0     0    0     0      0      0 --:--:--  0:00:01 --:--:--     0< HTTP/1.1 200 OK
< Date: Mon, 03 Nov 2014 16:35:48 GMT
< Server: Apache
< Vary: Cookie
< X-Frame-Options: SAMEORIGIN
< Connection: close
< Transfer-Encoding: chunked
< Content-Type: application/json
< 
{"GET_parameters": {"nhours": "2"}, "nrecords": 347, 
"errors": {}, 
"warnings": {"missingoptionalparameter": "Missing optional GET parameter starttime. Missing
  optional GET parameter endtime. Missing optional GET parameter mcp_cloud.
  Missing optional GET parameter computingsite. Missing optional GET parameter
  jobstatus. Missing optional GET parameter corecount. "}, 
"timestamp": "2014-11-03T16:35:49.675015", 
"query": {"JOBS INCLUDE: modificationtime__range": ["2014-11-03T14:35:48", "2014-11-03T16:35:48"]}, 
"data": [{"corecount": null, "merging": 0, "defined": 256, "throttled": 0, 
  "atlas_site": "Australia-ATLAS", "activated": 3, "running": 8, "assigned": 0, "failed": 5,
  "waiting": 0, "finished": 16, "computingsite": "ANALY_AUSTRALIA", "holding": 0, "sent": 1,
  "cancelled": 29, "transferring": 0, "pending": 0, "starting": 0, "cloud": "CA", 
  "mcp_cloud": "CA"}, ...]}
  ```

API: ?mcp_cloud=XYZ
-----------
The 'mcp_cloud' parameter is the 'cloud' field of the jobs tables (cloud to which a task was submitted), as oppose to the topology cloud to which the PanDA resource belongs. Wildcard is '*', e.g. '?mcp_cloud=C*N'.

Multiple 'mcp_cloud's can be filtered, comma is the delimiter. 

The API has 3 HTTP return states: 200, 404, 400.


**Example usage**:

 A single MCP cloud:
  ```
# curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://HOSTNAME/status_summary/?mcp_cloud=CERN"
* About to connect() to HOSTNAME port 80 (#0)
*   Trying IP-ADDRESS...   % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
  0     0    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0connected
> GET /status_summary/?cloud=CERN HTTP/1.1
> User-Agent: curl/7.22.0 (x86_64-pc-linux-gnu) libcurl/7.22.0 OpenSSL/1.0.1 zlib/1.2.3.4 libidn/1.23 librtmp/2.3
> Host: HOSTNAME
> Accept: application/json
> Content-Type: application/json
> 
  0     0    0     0    0     0      0      0 --:--:--  0:00:02 --:--:--     0< HTTP/1.1 200 OK
< Date: Mon, 03 Nov 2014 16:42:42 GMT
< Server: Apache
< Vary: Cookie
< X-Frame-Options: SAMEORIGIN
< Connection: close
< Transfer-Encoding: chunked
< Content-Type: application/json
  ```

Multiple MCP clouds:
  ```
# curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://HOSTNAME/status_summary/?mcp_cloud=CERN,TW"
* About to connect() to HOSTNAME port 80 (#0)
*   Trying IP-ADDRESS...   % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
  0     0    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0connected
> GET /status_summary/?cloud=CERN,TW HTTP/1.1
> User-Agent: curl/7.22.0 (x86_64-pc-linux-gnu) libcurl/7.22.0 OpenSSL/1.0.1 zlib/1.2.3.4 libidn/1.23 librtmp/2.3
> Host: HOSTNAME
> Accept: application/json
> Content-Type: application/json
> 
  0     0    0     0    0     0      0      0 --:--:--  0:00:02 --:--:--     0< HTTP/1.1 200 OK
< Date: Mon, 03 Nov 2014 16:43:29 GMT
< Server: Apache
< Vary: Cookie
< X-Frame-Options: SAMEORIGIN
< Connection: close
< Transfer-Encoding: chunked
< Content-Type: application/json
  ```

Non-existant MCP cloud:
  ```
# curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://HOSTNAME/status_summary/?mcp_cloud=blah"
* About to connect() to HOSTNAME port 80 (#0)
*   Trying IP-ADDRESS...   % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
  0     0    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0connected
> GET /status_summary/?mcp_cloud=blah HTTP/1.1
> User-Agent: curl/7.22.0 (x86_64-pc-linux-gnu) libcurl/7.22.0 OpenSSL/1.0.1 zlib/1.2.3.4 libidn/1.23 librtmp/2.3
> Host: HOSTNAME
> Accept: application/json
> Content-Type: application/json
> 
< HTTP/1.1 404 NOT FOUND
< Date: Mon, 03 Nov 2014 16:45:00 GMT
< Server: Apache
< Vary: Cookie
< X-Frame-Options: SAMEORIGIN
< Connection: close
< Transfer-Encoding: chunked
< Content-Type: application/json
< 
{ [data not shown]
100   531    0   531    0     0   4059      0 --:--:-- --:--:-- --:--:--  4500
* Closing connection #0
  ```

API: ?computingsite=XYZ
-----------
The 'computingsite' parameter is the 'computingsite' field of the jobs tables (PanDA resource where the job ran). Multiple 'computingsite's can be filtered, comma is the delimiter. Wildcard is '*', e.g. '?computingsite=ANALY*'.

The API has 3 HTTP return states: 200, 404, 400.

**Example usage**:

A single PanDA resource:
  ```
# curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://HOSTNAME/status_summary/?computingsite=CERN-PROD"
* About to connect() to HOSTNAME port 80 (#0)
*   Trying IP-ADDRESS...   % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
  0     0    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0connected
> GET /status_summary/?computingsite=CERN-PROD HTTP/1.1
> User-Agent: curl/7.22.0 (x86_64-pc-linux-gnu) libcurl/7.22.0 OpenSSL/1.0.1 zlib/1.2.3.4 libidn/1.23 librtmp/2.3
> Host: HOSTNAME
> Accept: application/json
> Content-Type: application/json
> 
  0     0    0     0    0     0      0      0 --:--:--  0:00:01 --:--:--     0< HTTP/1.1 200 OK
< Date: Mon, 03 Nov 2014 16:48:31 GMT
< Server: Apache
< Vary: Cookie
< X-Frame-Options: SAMEORIGIN
< Connection: close
< Transfer-Encoding: chunked
< Content-Type: application/json
{ [data not shown]
100   888    0   888    0     0    530      0 --:--:--  0:00:01 --:--:--   535
* Closing connection #0

  ```

Multiple PanDA resources:
  ```
# curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://HOSTNAME/status_summary/?computingsite=CERN-PROD,ANALY_CERN_SLC6"
* About to connect() to HOSTNAME port 80 (#0)
*   Trying IP-ADDRESS...   % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
  0     0    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0connected
> GET /status_summary/?computingsite=CERN-PROD,ANALY_CERN_SLC6 HTTP/1.1
> User-Agent: curl/7.22.0 (x86_64-pc-linux-gnu) libcurl/7.22.0 OpenSSL/1.0.1 zlib/1.2.3.4 libidn/1.23 librtmp/2.3
> Host: HOSTNAME
> Accept: application/json
> Content-Type: application/json
> 
< HTTP/1.1 200 OK
< Date: Mon, 03 Nov 2014 16:49:07 GMT
< Server: Apache
< Vary: Cookie
< X-Frame-Options: SAMEORIGIN
< Connection: close
< Transfer-Encoding: chunked
< Content-Type: application/json
< 
{ [data not shown]
100  1282    0  1282    0     0   1430      0 --:--:-- --:--:-- --:--:--  1544
* Closing connection #0
  ```

API: ?jobstatus=XYZ
-----------
The 'jobstatus' parameter is the 'jobstatus' field of the jobs tables. Multiple 'jobstatus's can be filtered, comma is the delimiter. 

The API has 3 HTTP return states: 200, 404, 400.

**Example usage**:

A single PanDA jobstatus:
  ```
# curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://HOSTNAME/status_summary/?jobstatus=defined"

  ```

Multiple PanDA jobstates:
  ```
# curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://HOSTNAME/status_summary/?jobstatus=defined,activated"

  ```

API: ?corecount=N
-----------
The 'corecount' parameter is the 'corecount' field of the schedconfig table, it is a property of the PanDA resource. Multiple 'corecount's can be filtered, comma is the delimiter. To filter corecount != N, please use '?corecount=-N'.

The webpage presents DB value corecount=NULL as 1. The JSON API presents value DB corecount=NULL as null.

The API has 3 HTTP return states: 200, 404, 400.

**Example usage**:

A single corecount:
  ```
# curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://HOSTNAME/status_summary/?corecount=8"

  ```

Multiple corecounts:
  ```
# curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://HOSTNAME/status_summary/?corecount=4,8"

  ```

API: ?cloud=XYZ
-----------
The 'cloud' parameter is the 'cloud' field of the schedconfig table, it is a property of the PanDA resource. Multiple 'clouds's can be filtered, comma is the delimiter. To filter cloud != XYZ, please use '?cloud=-XYZ'. You can use wildcard '*', e.g. '?cloud=CE*N'.

The API has 3 HTTP return states: 200, 404, 400.

**Example usage**:

A single cloud:
  ```
# curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://HOSTNAME/status_summary/?cloud=CERN"

  ```

Multiple clouds:
  ```
# curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://HOSTNAME/status_summary/?cloud=CERN,DE"

  ```

API: ?atlas_site=XYZ
-----------
The 'atlas_site' parameter is the 'gstat' field of the schedconfig table, it is a property of the PanDA resource. Multiple 'atlas_site's can be filtered, comma is the delimiter. To filter atlas_site != XYZ, please use '?atlas_site=-XYZ'. You can use wildcard '*', e.g. '?atlas_site=CERN*'.

The API has 3 HTTP return states: 200, 404, 400.

**Example usage**:

A single atlas_site:
  ```
# curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://HOSTNAME/status_summary/?atlas_site=CERN-PROD"

  ```

Multiple atlas_sites:
  ```
# curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://HOSTNAME/status_summary/?atlas_site=CERN-PROD,ANALY_CERN_SLC6"

  ```


API: ?status=XYZ
-----------
The 'status' parameter is the 'status' field of the schedconfig table, it is a property of the PanDA resource. Multiple 'status's can be filtered, comma is the delimiter. To filter status != XYZ, please use '?status=-XYZ'. 

The API has 3 HTTP return states: 200, 404, 400.

**Example usage**:

A single states:
  ```
# curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://HOSTNAME/status_summary/?status=online"

  ```

Multiple states:
  ```
# curl -H 'Accept: application/json' -H 'Content-Type: application/json' "http://HOSTNAME/status_summary/?status=online,brokeroff"

  ```


