ARG PYTHON_VERSION=3.14.7

FROM docker.io/almalinux:9
ARG PYTHON_VERSION
LABEL maintainer="PanDA team"

# IGTF CA repository (grid CA certificates)
RUN printf '%s\n' \
      '[carepo]' \
      'name=IGTF CA Repository' \
      'baseurl=https://linuxsoft.cern.ch/mirror/repository.egi.eu/sw/production/cas/1/current/' \
      'enabled=1' \
      'gpgcheck=1' \
      'gpgkey=https://dl.igtf.net/distribution/current/GPG-KEY-EUGridPMA-RPM-4' \
      > /etc/yum.repos.d/carepo.repo

# System packages
RUN yum install -y epel-release dnf-plugins-core && \
    dnf config-manager --set-enabled crb && \
    yum install -y httpd httpd-devel mod_ssl gridsite gcc git psmisc procps less which wget logrotate \
        fetch-crl ca-policy-egi-core net-tools nano libaio \
        openssl-devel readline-devel bzip2-devel libffi-devel zlib-devel \
        https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm && \
    yum install -y postgresql14 postgresql14-devel && \
    yum clean all && rm -rf /var/cache/yum

# Oracle Instant Client (pinned version)
RUN wget https://download.oracle.com/otn_software/linux/instantclient/2380000/oracle-instantclient-basic-23.8.0.25.04-1.el9.x86_64.rpm -P /tmp/ && \
    yum install -y /tmp/oracle-instantclient-basic-23.8.0.25.04-1.el9.x86_64.rpm && \
    wget https://download.oracle.com/otn_software/linux/instantclient/2380000/oracle-instantclient-sqlplus-23.8.0.25.04-1.el9.x86_64.rpm -P /tmp/ && \
    yum install -y /tmp/oracle-instantclient-sqlplus-23.8.0.25.04-1.el9.x86_64.rpm && \
    rm -f /tmp/oracle-instantclient-*.rpm && \
    yum clean all && rm -rf /var/cache/yum

# Python from source
RUN mkdir /tmp/python && cd /tmp/python && \
    wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xzf Python-${PYTHON_VERSION}.tgz && \
    cd Python-${PYTHON_VERSION} && \
    ./configure --enable-shared --enable-optimizations --with-lto && \
    make altinstall && \
    echo /usr/local/lib > /etc/ld.so.conf.d/local.conf && ldconfig && \
    cd / && rm -rf /tmp/python && \
    ln -s /usr/local/bin/python3.14 /usr/bin/python && \
    ln -s /usr/local/bin/pip3.14 /usr/bin/pip

ENV BIGMON_VIRTUALENV_PATH=/opt/bigmon \
    BIGMON_WSGI_PATH=/data/bigmon \
    DJANGO_SETTINGS_MODULE=core.settings

RUN adduser atlpan && groupadd zp && usermod -a -G zp atlpan

# venv and Python dependencies (only requirements.txt, not the whole repo)
COPY requirements.txt /tmp/requirements.txt
RUN python -m venv ${BIGMON_VIRTUALENV_PATH} && \
    ${BIGMON_VIRTUALENV_PATH}/bin/pip install --no-cache-dir -U pip setuptools gnureadline && \
    ${BIGMON_VIRTUALENV_PATH}/bin/pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm -f /tmp/requirements.txt

# load mod_wsgi built for the venv Python (avoids hardcoding the version-specific .so path)
RUN ${BIGMON_VIRTUALENV_PATH}/bin/mod_wsgi-express module-config | grep LoadModule > /etc/httpd/conf.modules.d/20-wsgi.conf

# Application code and Apache config
RUN mkdir -p ${BIGMON_WSGI_PATH}/config ${BIGMON_WSGI_PATH}/logs \
             ${BIGMON_VIRTUALENV_PATH}/etc/cert ${BIGMON_VIRTUALENV_PATH}/etc/idds && \
    rm -rf /etc/httpd/conf.d/*
COPY core ${BIGMON_WSGI_PATH}/core
COPY docker/start-daemon.sh /usr/local/bin/
COPY docker/conf.d/*.conf /etc/httpd/conf.d/

# late customization via config volume
RUN ln -fs ${BIGMON_WSGI_PATH}/config/local.py ${BIGMON_WSGI_PATH}/core/settings/local.py && \
    ln -fs ${BIGMON_WSGI_PATH}/config/tnsnames.ora /etc/tnsnames.ora

# Apache listens only on 8443
RUN sed -i '/^\s*Listen/d' /etc/httpd/conf/httpd.conf

# permissions: group 0 owns what the app writes at runtime
RUN mkdir -p /var/cache/mod_ssl /run/httpd && \
    chgrp -R 0 ${BIGMON_WSGI_PATH} ${BIGMON_VIRTUALENV_PATH}/etc /etc/httpd/conf.d /etc/grid-security \
                 /var/log/httpd /var/cache/mod_ssl && \
    chmod -R g=u ${BIGMON_WSGI_PATH} ${BIGMON_VIRTUALENV_PATH}/etc /etc/httpd/conf.d /etc/grid-security \
                 /run/httpd /var/log/httpd /var/cache/mod_ssl && \
    chmod +x /usr/local/bin/start-daemon.sh

ENTRYPOINT ["start-daemon.sh"]
STOPSIGNAL SIGWINCH
EXPOSE 8443
CMD ["all"]