from influxdb import InfluxDBClient
from kafka import SimpleProducer, KafkaClient

from bson import json_util
import yaml
import json

startdate = '2018-10-30'

# prepare Kafka producer
kafka = KafkaClient('blockchain-kafka-kafka.default.svc.cluster.local:9092')
producer = SimpleProducer(kafka)
print('kafka connection prepared')


# prepare InfluxDB connection
client = InfluxDBClient(host='influxdb-influxdb.default.svc.cluster.local', port=8086)
print('InfluxDB connection prepated')
	
# Switch database
client.switch_database('prices')
print('InfluxDB database connected')
	
# get list of measurements
measurements = client.get_list_measurements()

#clear measurements ending with '_USD'
clear_USD = [x for x in measurements 
               if x['name'].endswith('_USD') == False
              ]
#clear measurements ending with '_BTC'
clear_BTC = [x for x in clear_USD 
               if x['name'].endswith('_BTC') == False]

clear_BTC1 = [x for x in clear_BTC 
               if x['name'].endswith('coin') ]

#create test dataset
test_dict = [x for x in measurements if x['name'] == 'ALC_allcoin']			  

#try to read data from InfluxDB

#for x in test_dict: #this is just a simple dataset for testing connection
for x in clear_BTC: #use this to get all filtered measurements from InfluxDB
	print('handling ' + x['name'])
	results = client.query(('SELECT * from "%s" WHERE time >= now() - 1d LIMIT 2') %  x['name'])
	points = list(results.get_points())
	for i, p in enumerate(points):
		#add measure name
		points[i]['name'] = x['name']
		#send the query results to kafka
		producer.send_messages('test', json.dumps(points[i], default=json_util.default).encode('utf-8'))
	print(x['name'] + ' sent')	