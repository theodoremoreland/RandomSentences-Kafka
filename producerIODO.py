from confluent_kafka import Producer
from random import choice, randint
from time import sleep
import json

"""
don't forget to create a topic to post to!
    kafka-topics --bootstrap-server localhost:29092 --create --topic iodo --partitions 4 --replication-factor 1

Initialize your Python environment!
    python3 -m venv .venv
    source .venv/bin/activate
"""

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

#declare producer
p = Producer({'bootstrap.servers': 'localhost:29092'})

#initialize lists to append to
lstSubject = []
lstVerbIODO = []
lstIndirectObject = []
lstDirectObject = []

#read in txt files
with open('subject.txt') as txtSubject:
    for subject in txtSubject:
        lstSubject.append(subject.replace('\n',''))

with open('verbIODO.txt') as txtIODO:
    for verbIODO in txtIODO:
        lstVerbIODO.append(verbIODO.replace('\n',''))

with open('object.txt') as txtIndirectObject:
    for indirectObject in txtIndirectObject:
        lstIndirectObject.append(indirectObject.replace('\n',''))

with open('directObject.txt') as txtDirectObject:
    for directObject in txtDirectObject:
        lstDirectObject.append(directObject.replace('\n',''))

#with open('verbSVC.txt') as txtVerb:
#    for lines in txtVerb:
#        lstVerbComplementPairs.append(lines.replace('\n','').split(','))

#produce random sentences until you stop the stream and post them to the topic svc
while True:
    randIODO = f'{choice(lstSubject)} {choice(lstVerbIODO)} {choice(lstIndirectObject)} {choice(lstDirectObject)}'
    p.poll(0)
    p.produce('iodo', randIODO.encode('utf-8'), callback=delivery_report)
    #wait 3 seconds between producing sentences
    sleep(3)

#idk what this does but I know it needs to exist
p.flush()