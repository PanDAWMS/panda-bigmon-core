FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest
MAINTAINER PanDA team

RUN echo -e '[epel]\n\
name=Extra Packages for Enterprise Linux 9 [HEAD]\n\
baseurl=http://linuxsoft.cern.ch/epel/9/Everything/x86_64\n\
enabled=1\n\
gpgcheck=0\n\
gpgkey=http://linuxsoft.cern.ch/epel/RPM-GPG-KEY-EPEL-9\n\
exclude=collectd*,libcollectd*,mcollective,perl-Authen-Krb5,perl-Collectd,puppet,python*collectd_systemd*,koji*,python*koji*\n\
priority=20\' >> /etc/yum.repos.d/epel.repo

RUN echo -e '[carepo]\n\
name=IGTF CA Repository\n\
baseurl=https://linuxsoft.cern.ch/mirror/repository.egi.eu/sw/production/cas/1/current/\n\
enabled=1\n\
gpgcheck=0\n\
gpgkey=https://linuxsoft.cern.ch/mirror/repository.egi.eu/GPG-KEY-EUGridPMA-RPM-3\' >> /etc/yum.repos.d/carepo.repo

RUN ln -s /usr/bin/python3 /usr/bin/python && \
    ln -s /usr/bin/pip3 /usr/bin/pip

ENV BIGMON_VIRTUALENV_PATH /opt/bigmon
ENV BIGMON_WSGI_PATH /data/bigmon
ENV DJANGO_SETTINGS_MODULE core.settings

RUN yum -y update

RUN yum install -y nano python3-psycopg2 httpd.x86_64 conda gridsite mod_ssl.x86_64 httpd-devel.x86_64 gcc.x86_64 supervisor.noarch fetch-crl.noarch \
        python3 python3-devel less git ca-policy-egi-core \
        httpd-devel.x86_64 gcc.x86_64 supervisor.noarch fetch-crl.noarch wget net-tools sudo \
        http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient19.3-basic-19.3.0.0.0-2.x86_64.rpm \
        http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient19.3-devel-19.3.0.0.0-1.x86_64.rpm \
        http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient19.3-sqlplus-19.3.0.0.0-1.x86_64.rpm \
        http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient19.3-meta-19.3-3.el7.cern.x86_64.rpm \
        http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient-basic-19.3-3.el7.cern.x86_64.rpm \
        http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient-devel-19.3-3.el7.cern.x86_64.rpm \
        http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient-sqlplus-19.3-3.el7.cern.x86_64.rpm \
        http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient-tnsnames.ora-1.4.4-1.el7.cern.noarch.rpm \
        https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm && \
    yum install -y postgresql14 postgresql14-devel

RUN yum clean all && rm -rf /var/cache/yum

# setup env
RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan

RUN python3 -m venv ${BIGMON_VIRTUALENV_PATH} --system-site-packages

RUN ${BIGMON_VIRTUALENV_PATH}/bin/pip install --no-cache-dir --upgrade setuptools

RUN ${BIGMON_VIRTUALENV_PATH}/bin/pip install --no-cache-dir --upgrade channels pyOpenSSL daphne python-dotenv pyrebase4 confluent_kafka futures psycopg2-binary \
    aenum appdirs argcomplete asn1crypto attrs aws bcrypt \
    beautifulsoup4 boto3 bz2file cachetools certifi cffi chardet click codegen cryptography cx-Oracle cycler \
    dataclasses datefinder decorator defusedxml Django docopt dogpile.cache ecdsa \
    elasticsearch elasticsearch-dsl enum34 fabric findspark flake8 Flask futures google-auth html5lib httplib2 \
    humanize idds-client idds-common idds-workflow idna importlib-metadata iniconfig invoke ipaddress itsdangerous \
    Jinja2 joblib kiwisolver kubernetes linecache2 lxml MarkupSafe matplotlib mccabe mod-wsgi nose numpy oauthlib \
    olefile openshift packaging pandas paramiko patterns pep8 Pillow pip pluggy prettytable progressbar2 psutil \
    py pyasn1 pyasn1-modules pycodestyle pycparser pycrypto pyflakes PyJWT PyNaCl pyparsing pytest \
    python-dateutil python-magic python-openid python-social-auth python-string-utils python-utils python3-openid \
    pytz PyYAML redis regex reportlab requests requests-oauthlib rsa ruamel.yaml ruamel.yaml.clib rucio-clients \
    schedule scikit-learn scipy six sklearn  social-auth-core soupsieve sqlparse \
    stomp.py subprocess32 sunburnt tabulate threadpoolctl tiny-xslt toml traceback2 typing-extensions unittest2 \
    urllib3 webencodings websocket-client Werkzeug xlrd zipp \
    rucio-clients \
    django-bower django-cors-headers \
    django-datatables-view django-render-block django-tables2 django-templated-email djangorestframework \
    django-debug-toolbar django-extensions django-htmlmin django-mathfilters django-redis \
    django-redis-cache social-auth-app-django

RUN mkdir -p ${BIGMON_WSGI_PATH}
RUN mkdir ${BIGMON_WSGI_PATH}/config
RUN mkdir ${BIGMON_WSGI_PATH}/logs
RUN rm -rf /etc/httpd/conf.d/*

# copy tagged version or branch/fork snapshot from repository
COPY core ${BIGMON_WSGI_PATH}/core
COPY docker/activate_this.py ${BIGMON_VIRTUALENV_PATH}/bin/activate_this.py
COPY docker/start-daemon.sh /usr/local/bin/
COPY docker/conf.d/*.conf /etc/httpd/conf.d/
COPY docker/systemd/daphne.service /etc/systemd/system/daphne.service

# symlinks to allow late customization
RUN ln -fs ${BIGMON_WSGI_PATH}/config/local.py ${BIGMON_WSGI_PATH}/core/settings/local.py

# to work with non-root
RUN grep -v Listen /etc/httpd/conf/httpd.conf > /etc/httpd/conf/tmp; \
    echo Listen 8080 > /etc/httpd/conf/httpd.conf; \
    cat /etc/httpd/conf/tmp >> /etc/httpd/conf/httpd.conf; \
    rm /etc/httpd/conf/tmp

RUN chmod 777 ${BIGMON_WSGI_PATH}/logs
RUN chmod 777 /var/log/httpd
RUN chmod 777 /etc/grid-security
RUN chmod 777 /run/httpd

RUN chmod -R 777 /var/cache
RUN chmod -R 777 ${BIGMON_WSGI_PATH}/config
RUN chmod -R 777 /etc/httpd/conf.d

# to be removed for prodiction
RUN chmod -R 777 ${BIGMON_WSGI_PATH} && chmod -R 777 ${BIGMON_VIRTUALENV_PATH}

ENTRYPOINT ["start-daemon.sh"]

STOPSIGNAL SIGINT

EXPOSE 8443 8080 8000
CMD ["all"]
