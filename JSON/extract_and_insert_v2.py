import datetime
import gzip
import json
import postgresql
import re
import arrow
import sys

print(datetime.datetime.utcnow())
print("Job started")
# set working directories
# local 
#source_path = 'E:\\Alex\\python\\andmekaeve\\Alexey\\JSON\\'
# or remote
source_path = '\\\\sahver.et.ee\\DWH\\Export\\GoogleAnalytics\\Telia\\'

# set correct filename
yesterday=arrow.now().shift(days=-1).format('YYYYMMDD')
source_file = "ga_sessions_"+yesterday+".gz"

# try to read file content from gz archive
try:
	f = gzip.open(source_path+source_file, 'rt', encoding="utf8")

	print("Archive unzipped")
except:
	e = sys.exc_info()[0]
	print("Archive cannot be opened " +e)

#read zipped file	
#f = gzip.open('ga_sessions_20171128.gz', 'rt', encoding="utf8")
	
#read unzipped file content
#f = open('new_ga_sessions_20171128.json', encoding="utf8")
contents = f.read()

print("File loaded")
f.close()

# changing JSON format over
try:
	print("Adding commas")
	new_contents = contents.replace('\n', ',')
	del contents

	rm = new_contents.rstrip(',')
	del new_contents

	print("Adding [] brackets")
	output = ''.join(['[',rm.strip(),']'])
except:
	e = sys.exc_info()[0]
	print(str(e))	

#trying to save JSON output into a file
# try:
	# f = open("ga_sessions_"+yesterday+".json", 'w', encoding="utf8")
	# f.write(output)

	# print("output is saved to:"+source_file +".json")
	# f.close()
# except:
	# e = sys.exc_info()[0]
	# print(str(e))
	
#trying to load JSON
try:
	print("loading JSON")
	visits = json.loads(output)
	
	print("Loaded "+str(len(visits))+" visits")
except:
	e = sys.exc_info()[0]
	print(str(e))
	
#trying to connect to the database server
try:
	db = postgresql.open('pq://alex:P0stGr3!@darko.et.ee:5432/andmekaeve')
	print("Database connected")
except:
	e = sys.exc_info()[0]
	print("Database connection error: " +str(e))

	
#prepare INSERT
try:
	insertVisit = db.prepare("INSERT INTO json_extract11 (uniquevisitid, visitid, fullvisitorid, userid, visitstarttime, visitnumber, date, hitnumber, hit_time, hit_hour, hit_minute, hit_isentrance, hit_isexit, hit_isinteraction, latitude, longitude, pagepath, pagetitle, screenresolution, mobiledeviceinfo, devicecategory, language, campaign, keyword, medium, referral_path, source, bounces) "
                      "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28);")

	print("INSERT prepared")
except:
	e = sys.exc_info()[0]
	print("Problem with INSERT: " +e)
	
	
#try to INSERT data
print("INSERT start at: ")
print(datetime.datetime.utcnow())
for visit in visits:
    if re.match("^((?!GA).)*$", str(visit["userId"]) ) :
        hits = visit.pop('hits')
   
        for hit in hits:
            
            uniquevisitid = str( visit["visitId"]+visit["fullVisitorId"])
            visitid = str( visit["visitId"] ) 
            #print(visitid)
            fullvisitorid = str( visit["fullVisitorId"])
            #print(fullvisitorid)
            userid = str( visit["userId"] )
            #print(userid)
            visitstarttime = str( visit["visitStartTime"] )
            #print(visitstarttime)
            visitnumber = str( visit["visitNumber"])
            #print(visitnumber)
            date = str( visit["date"])
            #print(date)
            hitnumber = str( [hit['hitNumber']]  )
            #print(hitnumber)
            hit_time = str( [hit['time']]  )
            #print(hit_time)
            hit_hour = str( [hit['hour']]  )
            #print(hit_hour)
            hit_minute = str( [hit['minute']]  )
            #print(hit_minute)
            hit_isentrance = bool( hit.get('isEntrance', 'false') )
            #print(hit_isentrance)
            hit_isexit = bool( hit.get('isExit', 'false') )
            #print(hit_isexit)
            hit_isinteraction = bool( hit.get('isInteraction', 'false') )
            #print(hit_isinteraction)
            latitude = str( visit["geoNetwork"].get("latitude", 0) )
            #print(latitude)
            longitude = str( visit["geoNetwork"].get("longitude", 0) )
            #print(longitude)
            pagepath = str( [hit['page']['pagePath']]  )
            #print(pagepath)
            pagetitle = str( hit['page'].get('pageTitle', 0) )
            #print(pagetitle)
            screenresolution = str( visit["device"]["screenResolution"] )
            #print(screenresolution)
            mobiledeviceinfo = str( visit["device"]["mobileDeviceInfo"] )
            #print(mobiledeviceinfo)
            devicecategory = str( visit["device"]["deviceCategory"] )
            #print(devicecategory)
            language = str( visit["device"]["language"] )   
            #print(language)
            campaign = str( visit["trafficSource"]["campaign"] )
            #print(campaign)
            keyword = str( visit["trafficSource"].get("keyword", 0) )
            #print(keyword)
            medium = str( visit["trafficSource"]["medium"] )
            #print(medium)
            referral_path = str( visit["trafficSource"].get("referralPath", 0))
            #print(referral_path)
            
            try:
                if visit["trafficSource"]["source"] in visit:
                    source =  str(visit["trafficSource"]["source"] )
                    #print(source)
                else: 
                    source = "missing"
                    #print(source)
          
            except TypeError: 
                print("source received a TypeError")
            
            bounces = str(visit["totals"].get("bounces", "missing"))
                
            insertVisit(uniquevisitid,
                        visitid,
                        fullvisitorid,
                        userid,
                        visitstarttime,
                        visitnumber,
                        date,
                        hitnumber,
                        hit_time,
                        hit_hour,
                        hit_minute,
                        hit_isentrance,
                        hit_isexit,
                        hit_isinteraction,
                        latitude,
                        longitude,
                        pagepath,
                        pagetitle,
                        screenresolution,
                        mobiledeviceinfo,
                        devicecategory,
                        language,                        
                        campaign,
                        keyword,
                        medium,
                        referral_path,
                        source,
                        bounces)
            

#count table rows            
#db.query("select count(*) from json_extract10")

print("INSERT finished")

del output
del visits

#save output if needed
#new_output = output
#f = open('new_ga_sessions_20171128.json', 'w', encoding="utf8")
#f.write(new_output)
#f.close()
print(datetime.datetime.utcnow())
print("Job finished")