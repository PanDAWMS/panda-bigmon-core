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

ENV BIGMON_VIRTUALENV_PATH /opt/bigmon
ENV BIGMON_WSGI_PATH /opt/bigmon

RUN yum -y update

RUN yum install -y httpd.x86_64 gridsite mod_ssl.x86_64 httpd-devel.x86_64 gcc.x86_64 supervisor.noarch fetch-crl.noarch \
        python3 python3-devel less git httpd.x86_64 gridsite mod_ssl.x86_64 ca-policy-egi-core  \
        httpd-devel.x86_64 wget net-tools sudo \
        openssl-devel bzip2-devel libffi-devel\
        httpd-devel gcc-c++ make zlib-devel zlib postgresql postgresql-devel python-devel \
        https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-basic-linuxx64.rpm \
        https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-sqlplus-linuxx64.rpm \
        http://linuxsoft.cern.ch/cern/centos/7/cernonly/x86_64/Packages/oracle-instantclient-tnsnames.ora-1.4.4-1.el7.cern.noarch.rpm

RUN yum clean all && rm -rf /var/cache/yum

# install python3.10
RUN wget https://www.python.org/ftp/python/3.10.7/Python-3.10.7.tgz -P /tmp/ &&  tar -xzvf /tmp/Python-3.10.7.tgz -C /tmp && cd /tmp/Python-3.10.7 && ./configure --enable-optimizations --enable-shared && make -j4 && make altinstall
RUN echo '/usr/local/lib/' > /etc/ld.so.conf.d/python3.10.conf && ldconfig


# setup env
RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan

RUN pip3.10 install --upgrade pip virtualenv && cd /opt && virtualenv --python=/usr/local/bin/python3.10 bigmon

RUN ${BIGMON_VIRTUALENV_PATH}/bin/pip install --no-cache-dir --upgrade setuptools

RUN ${BIGMON_VIRTUALENV_PATH}/bin/pip install --no-cache-dir --upgrade aenum argcomplete asgiref attrs \
    autobahn Automat cachetools certifi cffi channels chardet charset-normalizer click confluent-kafka \
    constantly cryptography cx-Oracle cycler daphne decorator defusedxml Django django-datatables-view \
    django-debug-toolbar django-extensions djangorestframework docopt dogpile.cache elastic-transport \
    elasticsearch elasticsearch-dsl flake8 Flask fonttools google-auth humanize hyperlink idds-client \
    idds-common idds-workflow idna importlib-metadata incremental iniconfig itsdangerous Jinja2 joblib \
    jsonschema kiwisolver kubernetes linecache2 load-dotenv MarkupSafe matplotlib mccabe mod-wsgi nose \
    numpy oauthlib openshift packaging panda-client pandas pbr pep8 Pillow pluggy psutil psycopg2 py \
    pyasn1 pyasn1-modules pycodestyle pycparser pyflakes PyJWT pyOpenSSL pyparsing pyrsistent pytest \
    python-dateutil python-dotenv python-string-utils python3-openid pytz PyYAML reportlab requests \
    requests-oauthlib rsa rucio-clients scikit-learn scipy service-identity six social-auth-app-django \
    social-auth-core sqlparse stevedore stomp.py tabulate threadpoolctl tomli traceback2 Twisted txaio \
    typing_extensions unittest2 urllib3 uWSGI websocket-client Werkzeug zipp zope.interface

RUN mkdir -p ${BIGMON_WSGI_PATH}/pythonpath
RUN mkdir ${BIGMON_WSGI_PATH}/config
RUN mkdir ${BIGMON_WSGI_PATH}/logs
RUN rm -rf /etc/httpd/conf.d/*

# copy tagged version or branch/fork snapshot from repository
COPY core ${BIGMON_WSGI_PATH}/pythonpath/core
COPY docker/activate_this.py ${BIGMON_VIRTUALENV_PATH}/bin/activate_this.py
COPY docker/start-daemon.sh /usr/local/bin/
COPY docker/conf.d/*.conf /etc/httpd/conf.d/
COPY docker/systemd/daphne.service /etc/systemd/system/daphne.service
# to work with non-root
RUN grep -v Listen /etc/httpd/conf/httpd.conf > /etc/httpd/conf/tmp; \
    echo Listen 8080 > /etc/httpd/conf/httpd.conf; \
    cat /etc/httpd/conf/tmp >> /etc/httpd/conf/httpd.conf; \
    rm /etc/httpd/conf/tmp
RUN chmod 777 ${BIGMON_WSGI_PATH}/logs
RUN chmod 777 /var/log/httpd
RUN chmod 777 /etc/grid-security
RUN chmod 777 /run/httpd
RUN chmod 777 /etc/systemd/system/daphne.service
RUN chmod -R 777 /var/cache
RUN chmod -R 777 ${BIGMON_WSGI_PATH}/config
RUN chmod -R 777 /etc/httpd/conf.d
RUN chmod -R 777 /etc/systemd/system/daphne.service
# to be removed for prodiction
RUN chmod -R 777 ${BIGMON_WSGI_PATH} && chmod -R 777 ${BIGMON_VIRTUALENV_PATH}

ENTRYPOINT ["start-daemon.sh"]

STOPSIGNAL SIGINT

EXPOSE 8443 8080
CMD ["all"]
