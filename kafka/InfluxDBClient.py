def valid_date_type(arg_date_str):
    try:
        return datetime.datetime.strptime(arg_date_str, "%Y-%m-%d")
    except ValueError:
        msg = "Given Date ({0}) not valid! Expected format, YYYY-MM-DD!".format(arg_date_str)
        raise argparse.ArgumentTypeError(msg)

def valid_datetime_type(arg_datetime_str):
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
	
	import pandas as pd

	parser = argparse.ArgumentParser(description='InfluxDB data parser')
	parser.add_argument('-s', '--start-date',
						dest='start_date',
						type=valid_date_type,
                        default=None,
                        required=True,
                        help='Requered start date paramter in format "YYYY-MM-DD"')
	parser.add_argument('-e', '--end-date',
						dest='end_date',
						type=valid_date_type,
                        default=datetime.datetime.today(),
                        required=False,
                        help='Optional end date paramter in format "YYYY-MM-DD", Default = today()')
	args = parser.parse_args()
	start_datetime_object = args.start_date
	start_datetime = "'" + str(start_datetime_object) + "'" 
	print('Data extraction start datetime is ' + start_datetime)
	
	end_datetime_object = args.end_date
	end_datetime = "'" + str(end_datetime_object) + "'"
	print('Data extraction last datetime is ' + end_datetime)

	
	# generate date range
	data_range=pd.date_range(start_datetime, end_datetime)

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
	
	# date from data range
	for d in data_range:
		d=d.strftime('%Y-%m-%d')
		print('Processing date:' + d)
		
		# measure from measures list
		for x in test_dict: 
			
			print('Processing measure ' + x['name'])
			query = """SELECT * from "%s" WHERE time = '%s' """ %  (x['name'], d )
			print(query)
			results = client.query(query)
			points = list(results.get_points())
			print ("Query length : %d" % len (points) + " rows")

			for i, p in enumerate(points):
				#add measure name
				points[i]['name'] = x['name']
				print(points[i])
				#send the query results to kafka
				producer.send_messages('test', json.dumps(points[i], default=json_util.default).encode('utf-8'))
			print(x['name'] + ' for ' + d + ' is sent')	
		