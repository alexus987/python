def valid_date_type(arg_date_str):
    """custom argparse *date* type for user dates values given from the command line"""
    try:
        return datetime.datetime.strptime(arg_date_str, "%Y-%m-%d")
    except ValueError:
        msg = "Given Date ({0}) not valid! Expected format, YYYY-MM-DD!".format(arg_date_str)
        raise argparse.ArgumentTypeError(msg)
        
def valid_datetime_type(arg_datetime_str):
    """custom argparse type for user datetime values given from the command line"""
    try:
        return datetime.datetime.strptime(arg_datetime_str, "%Y-%m-%d %H:%M")
    except ValueError:
        msg = "Given Datetime ({0}) not valid! Expected format, 'YYYY-MM-DD HH:mm'!".format(arg_datetime_str)
        raise argparse.ArgumentTypeError(msg)        
        
        
if __name__ == '__main__':
	from influxdb import InfluxDBClient
	import json
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
	print('Data extraction start datetime is ' + start_datetime_object)

	# prepare InfluxDB connection
	client = InfluxDBClient(host='influxdb-influxdb.default.svc.cluster.local', port=8086)

	# Switch database
	client.switch_database('prices')

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
	test_dict = [x for x in measurements if x['name'] == 'ALC_allcoin']			  
	
	#generate output
	for x in clear_BTC:
	print(x['name'])
	results = client.query(('SELECT * from "%s" WHERE time >= %s ORDER by time DESC LIMIT 1') %  (x['name'], datetime))
	points = list(results.get_points())
	#write output to a file
	#data_file = x['name']+'.json'
	#with open(data_file, 'w') as outfile:
	#	json.dump(results.raw, outfile)
	for i, p in enumerate(points):
		#add measure name
		points[i]['name'] = x['name']
		print(points[i])
		#send the query results to kafka
		#producer.send_messages('test', json.dumps(points[i], default=json_util.default).encode('utf-8'))
	print(x['name'] + ' sent')	
	