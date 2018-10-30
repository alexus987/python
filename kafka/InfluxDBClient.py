def valid_datetime_type(arg_datetime_str):
    """custom argparse type for user datetime values given from the command line"""
    try:
        return datetime.datetime.strptime(arg_datetime_str, "%Y-%m-%d %H:%M")
    except ValueError:
        msg = "Given Datetime ({0}) not valid! Expected format, 'YYYY-MM-DD HH:mm'!".format(arg_datetime_str)
        raise argparse.ArgumentTypeError(msg)        
        
        
if __name__ == '__main__':
	from influxdb import InfluxDBClient
	from kafka import SimpleProducer, KafkaClient

	from bson import json_util
	import json
	import yaml
	import argparse
	import datetime

	parser = argparse.ArgumentParser(description='Example of custom type usage')
	parser.add_argument('-s', '--start-datetime',
						dest='start_datetime',
						type=valid_datetime_type,
                        default=None,
                        required=True,
                        help='start datetime in format "YYYY-MM-DD HH:mm"')
	args = parser.parse_args()
	start_datetime_object = args.start_datetime
	start_datetime = "'" + str(start_datetime_object) + "'" 
	print('Data extraction start datetime is ' + start_datetime)

	# prepare Kafka producer
	kafka = KafkaClient('blockchain-kafka-kafka.default.svc.cluster.local:9092')
	producer = SimpleProducer(kafka)
	print('Kafka connection prepared')

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
	
	#create test dataset
	#clear_BTC1 = [x for x in clear_BTC 
    #          if x['name'].endswith('coin') ]
			   
	#create another test dataset
	test_dict = [x for x in measurements if x['name'] == 'BTC_bitcoin']			  
	
	#generate output
	for x in test_dict: 
	
		print(x['name'])
		results = client.query(('SELECT * from "%s" WHERE time >= %s ORDER by time ASC ') %  (x['name'], start_datetime))
		points = list(results.get_points())

		for i, p in enumerate(points):
			#add measure name
			points[i]['name'] = x['name']
			#print(points[i])
			#send the query results to kafka
			producer.send_messages('test', json.dumps(points[i], default=json_util.default).encode('utf-8'))
		print(x['name'] + ' sent')	
		