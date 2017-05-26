#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author:       Matthew O'Keefe
# Date:         2017 May
#
# Description:  Query Elasticsearch to get F5 login events 

#              
#
# NOTE: we need to install gcc python-pip python-devel openldap-devel first
# Then we can use pip to install python-ldap xlsxwriter elasticsearch
#

import datetime
import ldap
import xlsxwriter
import os
import smtplib
import json
import elasticsearch

from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders

# define defaults
# define smtp parameters
SMTP_HOST = 'mail.DOMAIN.com'
SMTP_FROM = 'm_okeefe@DOMAIN.com'
SMTP_ADDR = ['m_okeefe@DOMAIN.com', ]
# SMTP_ADDR = ['m_okeefe@DOMAIN.com', ]
SMTP_FILE = '/data/projects/f5_logins/f5_logins.xlsx'

# define elasticsearch indexer
ES_HOST = 'ZED-elastic5.DOMAIN.com'
# define elasticsearch credentials
ES_USER = 'admin'
ES_PASS = 'xxxxxxx'

# define ldap parameters
AD_SERVER = 'ldap://ZED-dc1.DOMAIN.com:389'

# define credentials
AD_USER = 'CN=svc_elastic_ldap,OU=Vulcan,OU=Mingville,OU=Service Accounts,DC=DOMAIN,DC=com'
AD_PASS = 'XXXXXX'

REGION_DICT     = { \
'WESTERN Mingville' : ('OU=Users,OU=Western Mingville,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Shared Accounts,OU=Western Mingville,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com'), \
'EASTERN Mingville' : ('OU=Users,OU=Eastern Mingville,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Shared Accounts,OU=Eastern Mingville,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com'), \
'Mingville'         : ('CN=Clappers,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Systems Admins,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com'), \
'Oley'           : ('OU=Users,OU=Oley,OU=Oley Pacific,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Shared Accounts,OU=Oley,OU=Oley Pacific,OU=DOMAIN,DC=DOMAIN,DC=com'), \
'Moldavia'      : ('OU=Users,OU=Moldavia,OU=Oley Pacific,OU=DOMAIN,DC=DOMAIN,DC=com',), \
'Oley PACIFIC'   : ('OU=Systems Admins,OU=Oley Pacific,OU=DOMAIN,DC=DOMAIN,DC=com',), \
'Barsoom' : ('OU=Users,OU=Jumper,OU=Barsoom,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Shared Accounts,OU=Jumper,OU=Barsoom,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Shared Accounts,OU=Operations Centre,OU=Greebo,OU=Barsoom,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Users,OU=IT,OU=Operations Centre,OU=Greebo,OU=Barsoom,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Users,OU=Operations Centre,OU=Greebo,OU=Barsoom,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Users,OU=Napoleon,OU=Greebo,OU=Barsoom,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Shared Accounts,OU=Napoleon,OU=Greebo,OU=Barsoom,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Test Users,OU=Greebo,OU=Barsoom,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Users,OU=IT,OU=Napoleon,OU=Greebo,OU=Barsoom,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Users,OU=Isle of Man,OU=Barsoom,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Shared Accounts,OU=Isle of Man,OU=Barsoom,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Systems Admins,OU=Barsoom,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Arsenal,OU=Barsoom,OU=DOMAIN,DC=DOMAIN,DC=com'), \
'EUROPE'         : ('OU=Arsenal,OU=Europe,OU=DOMAIN,DC=DOMAIN,DC=com', \
'CN=Clappers,OU=Europe,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Users,OU=Europe,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Shared Accounts,OU=Europe,OU=DOMAIN,DC=DOMAIN,DC=com'), \
'UFO'            : ('OU=Users,OU=Dubai,OU=UFO,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Systems Admins,OU=UFO,OU=DOMAIN,DC=DOMAIN,DC=com', \
'CN=Clappers,OU=UFO,OU=DOMAIN,DC=DOMAIN,DC=com'), \
'BUM'         : ('OU=Users,OU=BUM,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Shared Accounts,OU=BUM,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Systems Admins,OU=BUM,OU=DOMAIN,DC=DOMAIN,DC=com', \
'CN=Clappers,OU=BUM,OU=DOMAIN,DC=DOMAIN,DC=com')\
}

REGION_DICT_Mingville = {'WESTERN Mingville' : ('OU=Users,OU=Western Mingville,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Shared Accounts,OU=Western Mingville,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com'), \
'EASTERN Mingville' : ('OU=Users,OU=Eastern Mingville,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Shared Accounts,OU=Eastern Mingville,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com'), \
'Mingville' : ('CN=Clappers,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Systems Admins,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com')\
}
	
REGION_DICT_WC = {'WESTERN Mingville' : ('OU=Users,OU=Western Mingville,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com',) }

REGION_DICT_SHORT = {'WESTERN Mingville' : ('OU=Users,OU=Western Mingville,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Shared Accounts,OU=Western Mingville,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com'), \
'EASTERN Mingville' : ('OU=Users,OU=Eastern Mingville,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Shared Accounts,OU=Eastern Mingville,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com'), \
'Mingville' : ('CN=Clappers,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Systems Admins,OU=Mingville,OU=DOMAIN,DC=DOMAIN,DC=com'), \
'BUM'         : ('OU=Users,OU=BUM,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Shared Accounts,OU=BUM,OU=DOMAIN,DC=DOMAIN,DC=com', \
'OU=Systems Admins,OU=BUM,OU=DOMAIN,DC=DOMAIN,DC=com', \
'CN=Clappers,OU=BUM,OU=DOMAIN,DC=DOMAIN,DC=com')}


class EsQueryStart:
	def __init__(self, ES_HOST, ES_PASS, ES_USER):
		# make connection to elasticsearch
		global f5_session_dict
		global user_master_dict
		global f5_valid_sessions
		print "Querying session starts...\n\n"
		
		es = elasticsearch.Elasticsearch(['https://' + ES_USER + ':' + ES_PASS + '@' + ES_HOST + ':9200'], verify_certs=False)
		
		# define time and query each index in date range
		for x in range (0,7):
			date = str(datetime.date.today() + datetime.timedelta(-x))
			dotdate = date[0:4] + '.' + date[5:7] + '.' + date[8:]
			# print "date = " , dotdate
			es_index = 'logstash-hsl-f5-' + dotdate
			# print "index = " , es_index
			
			# query login events for that day
			try:
				es_response_start = es.search(index=es_index, size=10000, body={"query": \
				{"bool" : { \
				"must" : [ { "match": { "apm_session_result" : "allow" }}], \
				"must_not" : [ { "match": { "apm_username" : "\"\"" }}] \
				}}, \
				"fields": ["apm_username","apm_sessionid", "client_ip", "host", "@timestamp", "geoip_src.country_name"]})
			except:
				print "elasticsearch query error"
				
			# write results to dictionary
			results_start = es_response_start["hits"]["hits"]
			
			
			for hit in results_start:
				#print "\n\n"
				#print hit["fields"]
				#print "\n\n"
				
				
				session_id_start = str(hit["fields"]["apm_sessionid"][0])
				account_name     = (str(hit["fields"]["apm_username"][0])).lower()
				
				print session_id_start
				
				try:
					location = str(hit["fields"]["geoip_src.country_name"][0])
				except:
					location = ""
				
				try:
					user_ou              = user_master_dict[account_name]["ou"]
				except:
					user_ou              = "unknown"
				try:
					user_region          = user_master_dict[account_name]["region"]
				except:
					user_region          = "unknown"
				try:
					given_name           = user_master_dict[account_name]["given_name"]
				except:
					given_name           = "unknown"
				try:
					surname              = user_master_dict[account_name]["surname"]
				except:
					surname              = "unknown"
				try:
					description          = user_master_dict[account_name]["description"]
				except:
					description          = "unknown"
				
				f5_session_record = { \
				"account_name"       : account_name, \
				"client_ip"          : str(hit["fields"]["client_ip"][0]), \
				"session_id_start"   : session_id_start, \
				"date_start"         : (str(hit["fields"]["@timestamp"][0])).split("T")[0], \
				"time_start"         : (str(hit["fields"]["@timestamp"][0])).split("T")[1], \
				"action_start"       : "allow", \
				"location"           : location, \
				"host"               : str(hit["fields"]["host"][0]), \
				"session_id_end"     : "", \
				"date_end"           : "", \
				"time_end"           : "", \
				"action_end"         : "", \
				"user_ou"            : user_ou, \
				"user_region"        : user_region, \
				"given_name"         : given_name, \
				"surname"            : surname, \
				"description"        : description \
				}
				
				f5_session_dict[session_id_start] = f5_session_record
				
				f5_valid_sessions.extend([session_id_start])
				
class EsQueryEnd:
	def __init__(self, ES_HOST, ES_PASS, ES_USER):
		print "Querying session ends...\n\n"
		# make connection to elasticsearch
		global f5_ia_session_dict
		es = elasticsearch.Elasticsearch(['https://' + ES_USER + ':' + ES_PASS + '@' + ES_HOST + ':9200'], verify_certs=False)
		
		# define time and query each index in date range
		for x in range (0,7):
			date = str(datetime.date.today() + datetime.timedelta(-x))
			dotdate = date[0:4] + '.' + date[5:7] + '.' + date[8:]
			# print "date = " , dotdate
			es_index = str('logstash-hsl-f5-' + dotdate)
			# print "index = " , es_index
		# query session end events for that day and correlate them
			for session in f5_ia_valid_sessions:
				search_parameters = {}
				search_parameters['index'] = es_index
				search_parameters['body'] = '{"query": {"bool" : { \
				"must" : [ { "match": { "apm_sessionid" : "' + session + '" }}], \
				"must_not" : [ { "match": { "apm_session_result" : "allow" }}] }}, \
				"fields": ["apm_username","apm_sessionid", "@timestamp", \
				"apm_session_result", "client_ip", "host", "geoip_src.country_name"]}'

				es_response_end = es.search(**search_parameters) 
				
				# write results to dictionary
				results_end = es_response_end["hits"]["hits"]
				
				# print "index = ", es_index
				# print "body = ", search_parameters['body']
				#print es_response_end
				
				print "\n\n"
				
				for hit in results_end:
					session_id_end = str(hit["fields"]["apm_sessionid"][0])
					account_name   = str(hit["fields"]["apm_username"][0])
					#print session_id_end
					
					try:
						date_end = (str(hit["fields"]["@timestamp"][0])).split("T")[0]
					except:
						pass
					
					try:
						time_end = (str(hit["fields"]["@timestamp"][0])).split("T")[1]
					except:
						pass
					
					try:
						action_end = str(hit["fields"]["apm_session_result"][0])
					except:
						action_end = ""
					try:
						f5_ia_session_dict[session_id_end]["session_id_end"] = session_id_end
						f5_ia_session_dict[session_id_end]["date_end"]       = date_end
						f5_ia_session_dict[session_id_end]["time_end"]       = time_end
						f5_ia_session_dict[session_id_end]["action_end"]     = action_end
					except:
						pass

class AdQuery:
	def __init__(self, ou, region, con):
		global user_master_dict
		print "Getting AD users....\n\n"
		# default query attributes
		FILTER = '(&(objectClass=user))'
		ATTRS = ['cn', 'description', 'sAMAccountName', 'givenName', 'sn', 'distinguishedName' ]
		
		#print "Searching OU = ", ou
		#search OU for computers
		results = con.search_s(ou, ldap.SCOPE_SUBTREE, FILTER , ATTRS) 
		#print "\n\n"
		# extract user data from AD results
		for CN in results:
			user_object            = CN[1]['distinguishedName'][0]
			user_cn                = CN[1]['cn'][0]
			try:
				account_name     = CN[1]['sAMAccountName'][0]
			except:
				account_name     = 'None'
			
			account_name_low     = account_name.lower()
			
			try:
				given_name         = CN[1]['givenName'][0]
			except:
				given_name         = 'None'
			
			try:
				surname            = CN[1]['sn'][0]
			except:
				surname            = 'None'
				
			try: 
				description = CN[1]['description'][0]
			except:
				description = "None"
			#
			try:
				unicode(user_cn, "ascii")
			except UnicodeError:
				user_cn = unicode(user_cn, "utf-8")
			else:
				pass
				
			try:
				unicode(account_name, "ascii")
			except UnicodeError:
				account_name = unicode(account_name, "utf-8")
			else:
				pass
				
			try:
				unicode(surname, "ascii")
			except UnicodeError:
				surname = unicode(surname, "utf-8")
			else:
				pass
				
			try:
				unicode(given_name, "ascii")
			except UnicodeError:
				given_name = unicode(given_name, "utf-8")
			else:
				pass

			try:
				unicode(description, "ascii")
			except UnicodeError:
				description = unicode(description, "utf-8")
			else:
				pass
			try:
				unicode(user_object, "ascii")
			except UnicodeError:
				user_object = unicode(user_object, "utf-8")
			else:
				pass
				

			try:
				given_name = user_cn.split(", ", 1)[1]
			except:
				given_name = ""
			# print given_name
			# generate computer dictionary
			user_dict_ad = { \
			"surname"           : surname, \
			"given_name"        : given_name, \
			"ou"                : ou, \
			"region"            : region, \
			"description"       : description, \
			"user_object"       : user_object, \
			"account_name"      : account_name_low \
			}
			# add to master dictionary
			user_master_dict[account_name_low] = user_dict_ad

class EsFilter:
	def __init__(self, f5_session_dict, f5_valid_sessions):
		global f5_ia_session_dict
		global f5_ia_valid_sessions
		for session in f5_valid_sessions:
			if ("Investment advisor" in f5_session_dict[session]["description"]) or \
			("Investment Advisor" in f5_session_dict[session]["description"]) or \
			("investment advisor" in f5_session_dict[session]["description"]) or \
			("IA" in f5_session_dict[session]["description"]):

				f5_ia_session_dict[session] = f5_session_dict[session]
				
				f5_ia_valid_sessions.extend([session])
				
class WriteXL:
	def __init__(self, f5_session_dict):
		print "Creating Spreadsheet....\n\n"
	
		# remove the old spreadsheet if it exists
		try:
			os.remove("/data/projects/f5_logins/f5_logins.xlsx")
		except OSError:
			pass
		
		# change directory
		os.chdir ('/data/projects/f5_logins')
	
		# create a spreadsheet of results	
		workbook = xlsxwriter.Workbook('f5_logins.xlsx')
	
		#####################################################
		# create first page
		# page ia f5 sessions
	
		worksheet = workbook.add_worksheet('IA_Sessions')
		worksheet.freeze_panes(1, 0)
	
		# Add a bold format to use to highlight cells.
		bold = workbook.add_format({'bold': True})
		bold_border = workbook.add_format({'bold': True , 'border': True})
		bold_rotate = workbook.add_format({'bold': True , 'border': True, 'rotation': '90', 'align': 'center', 'valign': 'center' })
	
		# Add a number format
		number_format = workbook.add_format({'num_format': '###0'})
	
		# Add an Excel date format
		date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})
	
		# write headers
		worksheet.write('A1',  'Session_ID Start', bold_rotate)
		worksheet.write('B1',  'Account Name', bold_rotate)
		worksheet.write('C1',  'Surname', bold_rotate)
		worksheet.write('D1',  'Given Name', bold_rotate)
		worksheet.write('E1',  'Description', bold_rotate)
		worksheet.write('F1',  'Start Date', bold_rotate)
		worksheet.write('G1',  'Start Time', bold_rotate)
		worksheet.write('H1',  'End Date', bold_rotate)
		worksheet.write('I1',  'End Time', bold_rotate)
		worksheet.write('J1',  'End Reason', bold_rotate)
		worksheet.write('K1',  'User OU', bold_rotate)
		worksheet.write('L1',  'User Region', bold_rotate)
		worksheet.write('M1',  'Location', bold_rotate)
		worksheet.write('N1',  'Host', bold_rotate)
		
		 # Start from the first cell below the headers.
		row = 1
		col = 0
	
		for session in f5_ia_session_dict:
	
			worksheet.write (row, col,      f5_ia_session_dict[session]["session_id_start"]   )
			worksheet.write (row, col + 1,  f5_ia_session_dict[session]["account_name"]   )
			worksheet.write (row, col + 2,  f5_ia_session_dict[session]["surname"],   )
			worksheet.write (row, col + 3,  f5_ia_session_dict[session]["given_name"] )
			worksheet.write (row, col + 4,  f5_ia_session_dict[session]["description"]  )
			worksheet.write (row, col + 5,  f5_ia_session_dict[session]["date_start"]     )
			worksheet.write (row, col + 6,  f5_ia_session_dict[session]["time_start"])
			worksheet.write (row, col + 7,  f5_ia_session_dict[session]["date_end"]                )
			worksheet.write (row, col + 8,  f5_ia_session_dict[session]["time_end"]            )
			worksheet.write (row, col + 9,  f5_ia_session_dict[session]["action_end"]             )
			worksheet.write (row, col + 10, f5_ia_session_dict[session]["user_ou"]          )
			worksheet.write (row, col + 11, f5_ia_session_dict[session]["user_region"]             )
			worksheet.write (row, col + 12, f5_ia_session_dict[session]["location"]       )
			worksheet.write (row, col + 13, f5_ia_session_dict[session]["host"]       )
	
			# write next row
			row += 1
		
		# insert filters on columns
		worksheet.autofilter(0, 0, row, 13)
	
	
		#####################################################
		# create second page
		# page 2 all f5 sessions
	
#		worksheet = workbook.add_worksheet('All_Sessions')
#		worksheet.freeze_panes(1, 0)
	
		# Add a bold format to use to highlight cells.
#		bold = workbook.add_format({'bold': True})
#		bold_border = workbook.add_format({'bold': True , 'border': True})
#		bold_rotate = workbook.add_format({'bold': True , 'border': True, 'rotation': '90', 'align': 'center', 'valign': 'center' })
	
		# Add a number format
#		number_format = workbook.add_format({'num_format': '###0'})
	
		# Add an Excel date format
#		date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})
	
		# write headers
#		worksheet.write('A1',  'Session_ID Start', bold_rotate)
#		worksheet.write('B1',  'Account Name', bold_rotate)
#		worksheet.write('C1',  'Surname', bold_rotate)
#		worksheet.write('D1',  'Given Name', bold_rotate)
#		worksheet.write('E1',  'Description', bold_rotate)
#		worksheet.write('F1',  'Start Date', bold_rotate)
#		worksheet.write('G1',  'Start Time', bold_rotate)
#		worksheet.write('H1',  'End Date', bold_rotate)
#		worksheet.write('I1',  'End Time', bold_rotate)
#		worksheet.write('J1',  'End Reason', bold_rotate)
#		worksheet.write('K1',  'User OU', bold_rotate)
#		worksheet.write('L1',  'User Region', bold_rotate)
#		worksheet.write('M1',  'Location', bold_rotate)
#		worksheet.write('N1',  'Host', bold_rotate)
		
		 # Start from the first cell below the headers.
#		row = 1
#		col = 0
	
#		for session in f5_session_dict:
	
#			worksheet.write (row, col,      f5_session_dict[session]["session_id_start"]   )
#			worksheet.write (row, col + 1,  f5_session_dict[session]["account_name"]   )
#			worksheet.write (row, col + 2,  f5_session_dict[session]["surname"],   )
#			worksheet.write (row, col + 3,  f5_session_dict[session]["given_name"] )
#			worksheet.write (row, col + 4,  f5_session_dict[session]["description"]  )
#			worksheet.write (row, col + 5,  f5_session_dict[session]["date_start"]     )
#			worksheet.write (row, col + 6,  f5_session_dict[session]["time_start"])
#			worksheet.write (row, col + 7,  f5_session_dict[session]["date_end"]                )
#			worksheet.write (row, col + 8,  f5_session_dict[session]["time_end"]            )
#			worksheet.write (row, col + 9,  f5_session_dict[session]["action_end"]             )
#			worksheet.write (row, col + 10, f5_session_dict[session]["user_ou"]          )
#			worksheet.write (row, col + 11, f5_session_dict[session]["user_region"]             )
#			worksheet.write (row, col + 12, f5_session_dict[session]["location"]       )
#			worksheet.write (row, col + 13, f5_session_dict[session]["host"]       )
#	
			# write next row
#			row += 1
		
		# insert filters on columns
#		worksheet.autofilter(0, 0, row, 13)

		# wrap it up
		workbook.close()
		
		send_xl = SendXL()
		
class SendXL:
	def __init__(self):
		print "Sending Spreadsheet....\n\n"

		msg = MIMEMultipart()
		msg['From'] = SMTP_FROM
		msg['To'] = ", ".join(SMTP_ADDR)
		msg['Date'] = formatdate(localtime = True)
		msg['Subject'] = 'F5 Investment Advisor Logins'
		body = "Hello,\n\nPlease see attached spreadsheet. \n\nThis shows F5 logins for the last 7 days, \
where the user object in AD has a description of 'IA' or 'Investment Advisor'. \n\n \
***This is an automatically generated message.***"

		content = MIMEText(body, 'plain')
		msg.attach(content)
		
		part = MIMEBase('application', "octet-stream")
		part.set_payload(open("f5_logins.xlsx", "rb").read())
		encoders.encode_base64(part)
		part.add_header('Content-Disposition', 'attachment; filename="f5_logins.xlsx"')
		msg.attach(part)
		
		smtp = smtplib.SMTP(SMTP_HOST, 25)
		smtp.sendmail(SMTP_FROM, SMTP_ADDR, msg.as_string())
		smtp.quit()
	
	
'''
"These pretzels are making me thirsty"
'''
			
if __name__ == "__main__":
	# execute only if run as a script
	# initialize dictionaries
	
	f5_session_dict      = {}
	f5_ia_session_dict   = {}
	user_master_dict     = {}
	f5_valid_sessions    = []
	f5_ia_valid_sessions = []
	
	con = ldap.initialize(AD_SERVER)
	con.simple_bind_s(AD_USER, AD_PASS)

	for region, ou_list in REGION_DICT.iteritems():
		for ou in ou_list:
			ad_query = AdQuery(ou, region, con)
	
	es_query_start = EsQueryStart(ES_HOST, ES_PASS, ES_USER)
	
	#print f5_valid_sessions
	
	# es_query_end   = EsQueryEnd(ES_HOST, ES_PASS, ES_USER)
	
	#print f5_session_dict
	
	es_filter = EsFilter(f5_session_dict, f5_valid_sessions)
	
	es_query_end = EsQueryEnd(ES_HOST, ES_PASS, ES_USER)
	
	write_excel = WriteXL(f5_session_dict)
	#print user_master_dict
#



