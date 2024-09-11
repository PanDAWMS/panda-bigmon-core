ARG PYTHON_VERSION=3.11.6

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

ARG PYTHON_VERSION

MAINTAINER PanDA team

RUN yum update -y
RUN yum install -y epel-release

RUN yum install -y httpd httpd-devel gcc gridsite git psmisc less wget logrotate procps which \
    openssl-devel readline-devel bzip2-devel libffi-devel zlib-devel systemd-udev

# install python
RUN mkdir /tmp/python && cd /tmp/python && \
    wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xzf Python-*.tgz && rm -f Python-*.tgz && \
    cd Python-* && \
    ./configure --enable-shared --enable-optimizations --with-lto && \
    make altinstall && \
    echo /usr/local/lib > /etc/ld.so.conf.d/local.conf && ldconfig && \
    cd / && rm -rf /tmp/pyton

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


# setup venv with pythonX.Y
RUN python$(echo ${PYTHON_VERSION} | sed -E 's/\.[0-9]+$//') -m venv ${BIGMON_VIRTUALENV_PATH}
RUN ${BIGMON_VIRTUALENV_PATH}/bin/pip install --no-cache-dir -U pip
RUN ${BIGMON_VIRTUALENV_PATH}/bin/pip install --no-cache-dir -U setuptools
RUN ${BIGMON_VIRTUALENV_PATH}/bin/pip install --no-cache-dir -U gnureadline
RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan
RUN mkdir /tmp/src
WORKDIR /tmp/src
COPY . .

RUN yum -y update

RUN yum install -y nano python3-psycopg2 httpd.x86_64 conda gridsite mod_ssl.x86_64 httpd-devel.x86_64 gcc.x86_64 supervisor.noarch fetch-crl.noarch \
        less git ca-policy-egi-core \
        httpd-devel.x86_64 gcc.x86_64 supervisor.noarch fetch-crl.noarch wget net-tools sudo \
        https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm && \
    yum install -y postgresql14 postgresql14-devel

# install Oracle Instant Client and tnsnames.ora
RUN wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-basic-linuxx64.rpm -P /tmp/ && \
    yum install /tmp/oracle-instantclient-basic-linuxx64.rpm -y && \
    wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-sqlplus-linuxx64.rpm -P /tmp/ && \
    yum install /tmp/oracle-instantclient-sqlplus-linuxx64.rpm -y

# Grab the latest version of the Oracle tnsnames.ora file
RUN ln -fs /data/bigmon/config/tnsnames.ora /etc/tnsnames.ora

RUN yum clean all && rm -rf /var/cache/yum

RUN ${BIGMON_VIRTUALENV_PATH}/bin/pip install --no-cache-dir --upgrade channels pyOpenSSL daphne python-dotenv pyrebase4 confluent_kafka futures psycopg2-binary \
    aenum appdirs argcomplete asn1crypto attrs aws bcrypt \
    beautifulsoup4 boto3 bz2file cachetools certifi cffi chardet click codegen cryptography oracledb cycler \
    dataclasses datefinder decorator defusedxml Django==5.0.8 docopt dogpile.cache ecdsa django-csp \
    elasticsearch elasticsearch-dsl opensearch-py enum34 fabric findspark flake8 Flask futures google-auth html5lib httplib2 \
    humanize idds-client idds-common idds-workflow idna importlib-metadata iniconfig invoke ipaddress itsdangerous \
    Jinja2 joblib kiwisolver kubernetes linecache2 lxml MarkupSafe matplotlib mccabe mod-wsgi nose numpy oauthlib \
    olefile openshift packaging pandas paramiko patterns pep8 Pillow pip pluggy prettytable progressbar2 psutil \
    py pyasn1 pyasn1-modules pycodestyle pycparser pycrypto pyflakes PyJWT PyNaCl pyparsing pytest \
    python-dateutil python-magic python-openid python-social-auth python-string-utils python-utils python3-openid \
    pytz PyYAML redis regex reportlab requests requests-oauthlib rsa ruamel.yaml ruamel.yaml.clib rucio-clients \
    schedule scikit-learn scipy six scikit-learn  social-auth-core soupsieve sqlparse \
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