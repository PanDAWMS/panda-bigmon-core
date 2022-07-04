FROM centos:centos7
MAINTAINER PanDA team

RUN yum -y update; yum clean all
RUN yum -y install sudo epel-release; yum clean all

RUN yum install -y httpd.x86_64 conda gridsite mod_ssl.x86_64 httpd-devel.x86_64 gcc.x86_64 supervisor.noarch fetch-crl.noarch postgresql postgresql-contrib postgresql-static postgresql-libs postgresql-devel && \
    yum clean all && \
    rm -rf /var/cache/yum

RUN yum install -y python3 python3-devel less git httpd.x86_64 conda gridsite mod_ssl.x86_64 httpd-devel.x86_64 gcc.x86_64 supervisor.noarch fetch-crl.noarch wget net-tools && \
    yum clean all && \
    rm -rf /var/cache/yum

RUN yum install -y http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient19.3-basic-19.3.0.0.0-2.x86_64.rpm http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient19.3-devel-19.3.0.0.0-1.x86_64.rpm http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient19.3-sqlplus-19.3.0.0.0-1.x86_64.rpm http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient19.3-meta-19.3-3.el7.cern.x86_64.rpm http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient-basic-19.3-3.el7.cern.x86_64.rpm http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient-devel-19.3-3.el7.cern.x86_64.rpm http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient-sqlplus-19.3-3.el7.cern.x86_64.rpm http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient-tnsnames.ora-1.4.4-1.el7.cern.noarch.rpm

RUN curl https://repository.egi.eu/sw/production/cas/1/current/repo-files/EGI-trustanchors.repo -o /etc/yum.repos.d/EGI-trustanchors.repo
RUN yum install -y fetch-crl.noarch ca-policy-egi-core && \
    yum clean all && \
    rm -rf /var/cache/yum

# setup env
RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan

RUN python3 -m venv /opt/panda
RUN source /etc/bashrc; /opt/panda/bin/pip install --no-cache-dir --upgrade django.js futures psycopg2
RUN /opt/panda/bin/pip install --no-cache-dir --upgrade pip
RUN /opt/panda/bin/pip install --no-cache-dir --upgrade setuptools

RUN /opt/panda/bin/pip install --no-cache-dir --upgrade psycopg2-binary

RUN /opt/panda/bin/pip install --no-cache-dir --upgrade aenum appdirs argcomplete asn1crypto attrs aws bcrypt beautifulsoup4 boto bz2file cachetools certifi cffi chardet click codegen cryptography cx-Oracle cycler dataclasses datefinder decorator defusedxml Django==2.2 django-bower django-cors-headers django-datatables-view django-debug-toolbar django-extensions django-htmlmin django.js django-mathfilters django-redis django-redis-cache django-render-block django-tables2 django-templated-email djangorestframework docopt dogpile.cache ecdsa elasticsearch elasticsearch-dsl enum34 fabric findspark flake8 Flask futures google-auth html5lib httplib2 humanize idds-client idds-common idds-workflow idna importlib-metadata iniconfig invoke ipaddress itsdangerous Jinja2 joblib kiwisolver kubernetes linecache2 lxml MarkupSafe matplotlib mccabe mod-wsgi nose numpy oauthlib olefile openshift packaging pandas paramiko patterns pep8 Pillow pip pluggy prettytable progressbar2 psutil psycopg2 py pyasn1 pyasn1-modules pycodestyle pycparser pycrypto pyflakes PyJWT PyNaCl pyparsing pytest python-dateutil python-magic python-openid python-social-auth python-string-utils python-utils python3-openid pytz PyYAML redis regex reportlab requests requests-oauthlib rsa ruamel.yaml ruamel.yaml.clib rucio-clients schedule scikit-learn scipy setuptools six sklearn social-auth-app-django social-auth-core soupsieve sqlparse stomp.py subprocess32 sunburnt tabulate threadpoolctl tiny-xslt toml traceback2 typing-extensions unittest2 urllib3 webencodings websocket-client Werkzeug xlrd zipp

RUN /opt/panda/bin/pip install --no-cache-dir --upgrade rucio-clients 

# Snapshot pandamon version
# TODO: Do a git clone of the pandamon repo
RUN mkdir /data
RUN mkdir /data/idds
RUN mkdir /data/idds/config
RUN mkdir /data/idds/logs
RUN chmod 777 /data/idds/logs

COPY docker/activate_this.py /opt/panda/bin/activate_this.py
COPY docker/panda-bigmon-core.tar.gz /data/idds/
COPY docker/http_conf.tar.gz /data/
RUN cd /data/idds; tar xzf panda-bigmon-core.tar.gz; ln -s panda-bigmon-core/core .
RUN cd /data; tar xzf http_conf.tar.gz; mv /etc/httpd/conf.d/ssl.conf /etc/httpd/conf.d/ssl.conf.origin; cp conf.d/* /etc/httpd/conf.d/

COPY docker/start-daemon.sh /usr/local/bin/
ENTRYPOINT ["start-daemon.sh"]


STOPSIGNAL SIGINT

EXPOSE 443
CMD ["all"]
