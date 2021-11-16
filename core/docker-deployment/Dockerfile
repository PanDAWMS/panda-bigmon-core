FROM python:3.7.1

# Installing Oracle instant client
WORKDIR    /opt/oracle
RUN         apt-get -y update && apt-get -y install python3 python3-pip libaio1 wget unzip git postgresql \
            && wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip \
            && unzip instantclient-basiclite-linuxx64.zip \
            && rm -f instantclient-basiclite-linuxx64.zip \
            && cd /opt/oracle/instantclient* \
            && rm -f *jdbc* *occi* *mysql* *README *jar uidrvci genezi adrci \
            && echo /opt/oracle/instantclient* > /etc/ld.so.conf.d/oracle-instantclient.conf \
            && ldconfig

WORKDIR    /data
COPY       ./requirements.txt /data/requirements.txt
RUN        mkdir -p "/opt/rucio/etc/"
COPY       ./rucio.cfg /opt/rucio/etc/rucio.cfg
RUN        pip3 install -r requirements.txt
RUN        git clone -b multi_setup https://github.com/PanDAWMS/panda-bigmon-core.git

EXPOSE     8000
ENV DJANGO_SETTINGS_MODULE settings
ENV X509_USER_PROXY /data/atlpan/x509up_u25606
ENV DEBUG=False
ENV PYTHONPATH "/data/panda-bigmon-core/core/settings:/data/panda-bigmon-core:/data/panda-bigmon-core/core"
ENTRYPOINT [ "/bin/bash", "-l", "-c" ]
CMD ["/bin/cp /tmp/settings/local.py /data/panda-bigmon-core/core/settings/.; env;cd /data/panda-bigmon-core/core; python manage.py runserver 0.0.0.0:8000; #/bin/bash"]
#docker run -it -v $(pwd)/settings:/tmp/settings -v $(pwd)/logs:/var/log/bingpanda -v /data/atlpan/:/data/atlpan/ -e DEPLOYMENT_BACKEND=POSTGRES -p 8000:8000 74c843a0047f