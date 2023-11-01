import socket, hashlib
from django.conf import settings

# BASEDIR = "/opt/python/kafka-clients-example/python/confluent-kafka-python/kerberos/"
# KAFKA_CLUSTER = "kafka-gp"
# TOPIC = "bigpanda_mon"
# PRINCIPAL = "aaleksee@CERN.CH"
# GROUP_ID = "aleksandr_group-" + TOPIC
# KEYTAB = BASEDIR + ".keytab"
# CACERTS = "/etc/pki/tls/certs/"

##################################

from confluent_kafka import Consumer

def initConsumer(client, jeditaskid):
    group = " ".join(str(x) for x in client)
    group = group + str(jeditaskid)
    group_id = hashlib.sha1(group.encode()).hexdigest()

    BOOTSTRAP_SERVERS = ",".join(
        map(lambda x: x + ":9093"
            , sorted([
                (socket.gethostbyaddr(i))[0] for i in (socket.gethostbyname_ex(settings.KAFKA.get('CLUSTER', None)+".cern.ch"))[2]
    ])
    )
    )
    # settings.KAFKA.get('GROUP_ID', None)
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'ssl.ca.location': settings.KAFKA.get('CACERTS', None),
        'security.protocol': 'SASL_SSL',
        'sasl.kerberos.keytab': settings.KAFKA.get('KEYTAB', None),
        'auto.offset.reset': 'latest',
        'enable.auto.offset.store': True,
        'sasl.kerberos.principal': settings.KAFKA.get('PRINCIPAL', None),
        'log_level': 1
    })
    consumer.subscribe([settings.KAFKA.get('TOPIC', None)])

    return consumer



