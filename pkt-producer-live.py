from time import sleep
from struct import *
from kafka import KafkaProducer
import pcapy
import socket
from ip2geotools.databases.noncommercial import DbIpCity

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: x.encode('utf-8'))

print("Created Producer\n")

cap = pcapy.open_live("\\Device\\NPF_{92A73785-3D32-4F12-8559-5725D685BED2}", 65536, 1, 0)

print("Started Capture\n")

while True:
    (header, payload) = cap.next()

    ipHeader = payload[14:34]
    ip_hdr = unpack("!12s4s4s", ipHeader)
    srcIp = socket.inet_ntoa(ip_hdr[1])
    dstIp = socket.inet_ntoa(ip_hdr[2])
    response = DbIpCity.get(srcIp, api_key='free')

    msg = str(response.country) + ', ' + str(response.city) + ', ' + str(response.latitude) + ', ' + str(response.longitude) + ', ' + str(response.ip_address)

    if len(str(response.country)) > 1 and str(response.city) != 'None':
        print(str(response.country) + ', ' + str(response.city) + ', ' + str(response.latitude) + ', ' + str(response.longitude) + ', ' + str(response.ip_address))
        producer.send('pkttest', msg)
    sleep(0.5)
