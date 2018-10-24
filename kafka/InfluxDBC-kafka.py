from influxdb import InfluxDBClient
from kafka import SimpleProducer, KafkaClient

from bson import json_util
import yaml
import json

# prepare Kafka producer
try:
	kafka = KafkaClient('blockchain-kafka-kafka.default.svc.cluster.local:9092')
	producer = SimpleProducer(kafka)
	print('kafka connection is OK')
except Exception as ex:
    print(ex)

# Connect to InfluxDB
try:
	client = InfluxDBClient(host='influxdb-influxdb.default.svc.cluster.local', port=8086)
	print('InfluxDB connection prepated')
except Exception as ex:
    print(ex)
	
# Switch database
try:
	client.switch_database('prices')
	print('InfluxDB database connected')
except Exception as ex:
    print(ex)
	
# get list of measurements
measurements = client.get_list_measurements()

#clear measurements ending with '_USD'
clear_USD = [x for x in measurements 
               if x['name'].endswith('_USD') == False
              ]
#clear measurements ending with '_BTC'
clear_BTC = [x for x in clear_USD 
               if x['name'].endswith('_BTC') == False
              ]

#create test dataset
test_dict = [x for x in measurements if x['name'] == '611_USD']			  

#try to read data from InfluxDB
try:
	for x in test_dict: #this is just a simple dataset for testing connection
	#for x in clear_BTC: #use this to get all filtered measurements from InfluxDB
		print('handling ' + x['name'])
		results = client.query(('SELECT * from "%s" ORDER by time DESC LIMIT 1') %  x['name'])
		points = list(results.get_points())
		#uncomment the row below to send the query results to kafka
		#producer.send_messages('test', json.dumps(points, indent=4, default=json_util.default).encode('utf-8'))
		
		#save results into a file
		data_file = x['name']+'.json'
		with open(data_file, 'w') as outfile:
			json.dump(points, outfile)
			print(data_file + ' saved')
except Exception as ex:
    print(ex)

#try to send data to kafka
try:
	with open(data_file) as f:
		for line in f:
			d = yaml.safe_load(line)
			jd = json.dumps(d)
			#send message
			producer.send_messages('test', json.dumps(jd, indent=4, default=json_util.default).encode('utf-8'))
			print('message sent')
except Exception as ex:
	print(ex)