from influxdb import InfluxDBClient
import json

# Connect to InfluxDB
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
               if x['name'].endswith('_BTC') == False
              ]
#generate output
for x in clear_BTC:
	print(x['name'])
	results = client.query(('SELECT * from "%s" ORDER by time DESC LIMIT 1') %  x['name'])
	data_file = x['name']+'.json'
	with open(data_file, 'w') as outfile:
		json.dump(results.raw, outfile)          
#results.raw