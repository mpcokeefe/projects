#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author:       Matthew O'Keefe
# Date:         2017 April
#
# Description:  Get list of computers from specific containers in ad;
#               get list of sensors from carbon black and match last contact time;
#               get list of endpoints from mcafee sql and match last contact time;
#               get list of endpoints from sccm with hardware and software scan times
#               and machine details and sccm compliance status;
#               check for bitlocker in ad for laptops;
#               aggregate data and output to excel as tables and charts;
#               add descriptionb from ad and serial number from sccm;
#               output to elasticsearch
#               April: added CI IT workstations
#               April: added check for duplicate carbon black objects
#
# NOTE: we need to install gcc python-pip python-devel openldap-devel first
#
# Then we can use pip to install python-ldap cbapi pymssql xlsxwriter elasticsearch
#
#
#

import datetime
import ldap
import elasticsearch
import cbapi
import pymssql
# import csv
import xlsxwriter
import os
import smtplib
import json

from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders

# define the OU we want to scan for computers

REGION_DICT     = { \
'WESTERN VULCAN' : ('OU=Laptops,OU=Western VULCAN,OU=VULCAN,OU=domain,DC=domain,DC=com', \
'OU=Workstations,OU=Western VULCAN,OU=VULCAN,OU=domain,DC=domain,DC=com'), \
'EASTERN VULCAN' : ('OU=Laptops,OU=Eastern VULCAN,OU=VULCAN,OU=domain,DC=domain,DC=com', \
'OU=Workstations,OU=Eastern VULCAN,OU=VULCAN,OU=domain,DC=domain,DC=com'), \
'ASIA'           : ('OU=Laptops,OU=Asia,OU=Asia Pacific,OU=domain,DC=domain,DC=com', \
'OU=Workstations,OU=Asia,OU=Asia Pacific,OU=domain,DC=domain,DC=com'), \
#'AUSTRALIA'      : '', \
'BARSOOM' : ('OU=Laptops,OU=JUMPER,OU=BARSOOM,OU=domain,DC=domain,DC=com', \
'OU=Workstations,OU=JUMPER,OU=BARSOOM,OU=domain,DC=domain,DC=com', \
'OU=Workstations,OU=IT,OU=JUMPER,OU=BARSOOM,OU=domain,DC=domain,DC=com', \
'OU=Laptops,OU=Operations Centre,OU=GRINGO,OU=BARSOOM,OU=domain,DC=domain,DC=com', \
'OU=Workstations,OU=Operations Centre,OU=GRINGO,OU=BARSOOM,OU=domain,DC=domain,DC=com', \
'OU=Workstations,OU=IT,OU=Operations Centre,OU=GRINGO,OU=BARSOOM,OU=domain,DC=domain,DC=com', \
'OU=Laptops,OU=Napoleon,OU=GRINGO,OU=BARSOOM,OU=domain,DC=domain,DC=com', \
'OU=Workstations,OU=Napoleon,OU=GRINGO,OU=BARSOOM,OU=domain,DC=domain,DC=com', \
'OU=Workstations,OU=IT,OU=Napoleon,OU=GRINGO,OU=BARSOOM,OU=domain,DC=domain,DC=com', \
'OU=Laptops,OU=Isle of Man,OU=BARSOOM,OU=domain,DC=domain,DC=com', \
'OU=Workstations,OU=Isle of Man,OU=BARSOOM,OU=domain,DC=domain,DC=com'), \
'VENUS'         : ('OU=Banana,OU=VENUS,OU=domain,DC=domain,DC=com', \
'OU=Laptops,OU=VENUS,OU=domain,DC=domain,DC=com', 'OU=Workstations,OU=VENUS,OU=domain,DC=domain,DC=com'), \
'UFO'            : ('Ou=Laptops,OU=DoogieHowzer,OU=UFO,OU=domain,DC=domain,DC=com', \
'OU=Workstations,OU=DoogieHowzer,OU=UFO,OU=domain,DC=domain,DC=com', \
'OU=Offline Workstations (E),OU=DoogieHowzer,OU=UFO,OU=domain,DC=domain,DC=com'), \
'ITV'         : ('OU=Laptops,OU=ITV,OU=domain,DC=domain,DC=com', \
'OU=Workstations,OU=ITV,OU=domain,DC=domain,DC=com')\
}

RETIRED_LIST     = ['CN=Workstations,CN=Retired,DC=domain,DC=com', ]
OFFLINE_LIST     = ['OU=Offline Workstations,OU=Western VULCAN,OU=VULCAN,OU=domain,DC=domain,DC=com', 'OU=DR Workstations,OU=Western VULCAN,OU=VULCAN,OU=domain,DC=domain,DC=com' \
'OU=Offline Workstations (E),OU=Eastern VULCAN,OU=VULCAN,OU=domain,DC=domain,DC=com', 'OU=DR Workstations (E),OU=Eastern VULCAN,OU=VULCAN,OU=domain,DC=domain,DC=com' ]

# BASE_DN = 'CN=Workstations,CN=Retired,DC=domain,DC=com'


# define credentials
AD_USER = 'CN=svc_elastic_ldap,OU=Moldavia,OU=VULCAN,OU=Service Accounts,DC=domain,DC=com'
AD_PASS = 'XXXXXXX'


# define ldap server for lookups
AD_SERVER = 'ldap://zed-dc2.domain.com:389'
FILTER = '(&(objectClass=computer))'
ATTRS = ['cn', 'userAccountControl', 'description', 'operatingsystem']
# ATTRS = ['cn']

# define bitlocker query parameters
BL_FILTER = "(&(objectlass=*))"
BL_ATTRS  = ['cn']

# define carbon black connection parameters
CBURL = 'https://cb.domain.com:8443'
API_TOKEN = 'xyxyxyxyxyxyxyxyxy'

# define elasticsearch indexer
ES_HOST = 'zed-linuxg.domain.com'

# define elasticsearch credentials
ES_USER = 'admin'
ES_PASS = 'elastic'

# define mcafee mssql connection parameters
# MC_HOST = 'ZINGBAT-AG-MCAFEE.domain.com'
MC_HOST1 = '10.1.1.62'
MC_HOST2 = '10.1.1.62'
MC_USER = 'McAfeeSQL_Read'
MC_PASS = 'xxxxxxxxxxxxxxxx!'
MC_DB   = 'McAfee_EPO_DB_PROD'


# define sccm mssql connection parameters
SCCM_HOST = 'zed-sccm.domain.com'
SCCM_USER = 'MYDOMAIN\BUMBOY'
SCCM_PASS = 'xxxxxxxxxxxxxxxxxxxxxx'
SCCM_DB   = 'BUMDB'

# define smtp paramters
SMTP_HOST = 'mail.domain.com'
SMTP_FROM = 'm_okeefe@domain.com'
SMTP_ADDR_ALL = ['m_okeefe@domain.com', \
]
SMTP_ADDR_MGURNEY = ['m_okeefe@domain.com', 'NikNak@domain.com' ]
SMTP_FILE = '/data/projects/computer_state/computer_state.xlsx'

# initialize global variables
computer_dict_all = {}
computer_dict_cb  = {}
computer_dict_mc  = {}
computer_dict_sc  = {}


# initialize counters for ou and regions 
ou_all = []
ou_count = 0
ou_scores_dict = {}

region = []
region_all = []
region_count = 0
region_scores_dict = {}


# define time
now_minus_30_days = datetime.date.today() + datetime.timedelta(-30)

# establish ldap connection
con = ldap.initialize(AD_SERVER)
con.simple_bind_s(AD_USER, AD_PASS)

# define function to convert ldap timestamp to human readable format
def convert_ldaptime(ldaptime):
	unixtime = ((int(ldaptime)/10000000)-11644473600)
	return (datetime.datetime.fromtimestamp(int(unixtime)).strftime('%Y-%m-%d %H:%M:%S'))

	
	
# get computers from AD to form basis of our master dictionary of computers

def get_computers_ad(ou, region):
	# name, ad_status, ou, region, hardware, bl_status, description
	# 0     1          2   3       4         5          6
	# reference global variables
	global computer_dict_all
	global FILTER
	global ATTRS
	global bl_status
	
	# search OU for computers
	results = con.search_s(ou, ldap.SCOPE_SUBTREE, FILTER , ATTRS) 

	# extract CN for computer object from results
	for CN in results:
		computer_cn = CN[1]['cn'][0]
		userAccountControl = CN[1]['userAccountControl'][0]
		computer_object = CN[0]
		try: 
			ad_description = CN[1]['description'][0]
		except:
			ad_description = "None"
		
		try:
			unicode(ad_description, "ascii")
		except UnicodeError:
			ad_description = unicode(ad_description, "utf-8")
		else:
			pass
		
		try:
			ad_os = CN[1]['operatingSystem'][0]
		except:
			ad_os = "None"
		
		# append Mac OS to description
		if ("Mac" or "mac") in ad_os:
			ad_description = "*** " + ad_os + " *** : " + ad_description
		
		try:
			unicode(ad_os, "ascii")
		except UnicodeError:
			ad_os = unicode(ad_os, "utf-8")
		else:
			pass
		
		if ("laptop" in CN[0]) or ("Laptop" in CN[0]):
			hardware = "Laptop"
			# get bitlocker status if not a Mac
			if not ("Mac" in ad_os): 
				bl_results  = con.search_s(CN[0], ldap.SCOPE_ONELEVEL)
				# print(bl_results)
				if not bl_results:
					bl_status = 1
				else:
					bl_status = 0
			else:
				bl_status = 0
		else:
			hardware  = "Desktop"
			bl_status = 0

			# generate computer dictionary
		computer_dict_ad = { \
		"cn": computer_cn, \
		"ad_status": userAccountControl, \
		"ou": ou, \
		"region": region, \
		"hardware": hardware, \
		"bl_status": bl_status, \
		"desc": ad_description, \
		"ad_os": ad_os, \
		"cb_status": 1, \
		"cb_id": "", \
		"cb_group": "", \
		"cb_os": "", \
		"cb_datetime": "", \
		"cb_date": "", \
		"cb_date_check": 1, \
		"mc_status": 1, \
		"mc_datetime": "", \
		"mc_date": "", \
		"mc_date_check": 1, \
		"sc_user": "", \
		"sc_site": "", \
		"sc_os": "", \
		"sc_sp": "", \
		"sc_status": 1, \
		"sc_compliance": "", \
		"sc_hw_datetime": "", \
		"sc_sw_datetime": "", \
		"sc_hw_date": "", \
		"sc_sw_date": "", \
		"sc_hw_date_check": 1, \
		"sc_sw_date_check": 1, \
		"sc_comp_check": 1, \
		"sc_serial_number": "", \
		"sc_missing_patches": "unknown",
		}
		
		# add computer dictionary to ad dictionary
		computer_dict_all[computer_cn] = computer_dict_ad

#
#get carbon black data and add to master dictionary 
#
def get_computers_cb():
	# name, cb_id, cb_group, cb_os, cb_datetime, cb_date, cb_date_check
	# 0     1      2         3      4            5        6
	print "\n\nQuerying Carbon Black....\n\n"
	# reference global variables
	global computer_dict_all
	
	# connect to cb server
	cb = cbapi.CbApi(CBURL, token=API_TOKEN, ssl_verify=False)
	# get sensors
	sensors = cb.sensors()
	
	for sensor in sensors:
		try:
			sensor_date = sensor['last_checkin_time'].split(" ", 1)[0]
		except:
			sensor_date = ""
		
		#computer_list_cb_uni.append([sensor['computer_name'], sensor['id'], sensor['group_id'], sensor['os_environment_display_string'], sensor['last_checkin_time'], sensor_date])

		computer_dict_cb = {\
		"cb_cn": str(sensor['computer_name']), \
		"cb_id": sensor['id'], \
		"cb_group": sensor['group_id'], \
		"cb_os": str(sensor['os_environment_display_string']), \
		"cb_datetime": str(sensor['last_checkin_time']), \
		"cb_date": str(sensor_date) }
		
		date_obj = datetime.datetime.strptime(sensor_date, '%Y-%m-%d')
		if date_obj.date() > now_minus_30_days:
			computer_dict_cb['cb_date_check'] = 0
		else:
			computer_dict_cb['cb_date_check'] = 1
		
		# print(computer_dict_cb)
		# check and append to master dictionary
		for computer in computer_dict_all:
			# find a match
			if computer_dict_all[computer]["cn"] == computer_dict_cb["cb_cn"]:
				# look for any existing data for that computer
				# if no data then add the cb data
				if computer_dict_all[computer]["cb_date"] == "":
					computer_dict_all[computer]["cb_status"]     = 0
					computer_dict_all[computer]["cb_id"]         = computer_dict_cb["cb_id"]
					computer_dict_all[computer]["cb_group"]      = computer_dict_cb["cb_group"]
					computer_dict_all[computer]["cb_os"]         = computer_dict_cb["cb_os"]
					computer_dict_all[computer]["cb_datetime"]   = computer_dict_cb["cb_datetime"]
					computer_dict_all[computer]["cb_date"]       = computer_dict_cb["cb_date"]
					computer_dict_all[computer]["cb_date_check"] = computer_dict_cb["cb_date_check"]
				else:
					# if data then compare existing date with cb date and choose latest one
					current_date_obj = datetime.datetime.strptime(computer_dict_all[computer]["cb_date"],'%Y-%m-%d') 
					# print current_date_obj
					if current_date_obj.date() < date_obj.date():
						computer_dict_all[computer]["cb_status"]     = 0
						computer_dict_all[computer]["cb_id"]         = computer_dict_cb["cb_id"]
						computer_dict_all[computer]["cb_group"]      = computer_dict_cb["cb_group"]
						computer_dict_all[computer]["cb_os"]         = computer_dict_cb["cb_os"]
						computer_dict_all[computer]["cb_datetime"]   = computer_dict_cb["cb_datetime"]
						computer_dict_all[computer]["cb_date"]       = computer_dict_cb["cb_date"]
						computer_dict_all[computer]["cb_date_check"] = computer_dict_cb["cb_date_check"]
				
#
# get mcafee data and add to master dictionary
#
def get_computers_mc():
	# name, mc_datetime, mc_date, mc_date_check
	# 0     1            2        3
	#
	print "\n\nQuerying McAfee....\n\n"
	# reference global variables
	global computer_dict_all

	# connect to mcafee server
	try:
		mc_conn = pymssql.connect( server=MC_HOST1, port=14401, user=MC_USER, password=MC_PASS, database=MC_DB)
	except:
  	        mc_conn = pymssql.connect( server=MC_HOST2, port=14401, user=MC_USER, password=MC_PASS, database=MC_DB)
        mc_conn
        cursor = mc_conn.cursor()

	
	# execute query
	cursor.execute("SELECT NODENAME,LASTUPDATE from dbo.EPOLeafNode")
	columns = [column[0] for column in cursor.description]
	
	
	for row in cursor.fetchall():
		mc_computer = str(row[0])
		
		# explictly clear values
		
		# write values if present
		computer_dict_mc[mc_computer] = { \
		"mc_cn": str(row[0]), \
		"mc_datetime": str(row[1]) \
		}
	
		# print(computer_dict_mc)
	
		# remove "None" entries
		# and create date field
		if computer_dict_mc[mc_computer]["mc_datetime"] != "None":
			try:
				computer_dict_mc[mc_computer]["mc_datetime"] = computer_dict_mc[mc_computer]["mc_datetime"] + "-00:00"
				computer_dict_mc[mc_computer]["mc_date"]     = computer_dict_mc[mc_computer]["mc_datetime"].split(" ", 1)[0]
		
				# perform date check
				date_obj = datetime.datetime.strptime(computer_dict_mc[mc_computer]["mc_date"], '%Y-%m-%d')
				if date_obj.date() > now_minus_30_days:
					computer_dict_mc[mc_computer]["mc_date_check"] = 0
				else:
					computer_dict_mc[mc_computer]["mc_date_check"] = 1
			except:
				computer_dict_mc[mc_computer]["mc_date"] = ""
				computer_dict_mc[mc_computer]["mc_date_check"] = 1
		else:
			computer_dict_mc[mc_computer]["mc_date"] = ""
			computer_dict_mc[mc_computer]["mc_date_check"] = 1
			
		# print(computer_dict_mc)
		# check and append to master dictionary
		for computer in computer_dict_all:
			if computer_dict_all[computer]["cn"] == computer_dict_mc[mc_computer]["mc_cn"]:
				computer_dict_all[computer]["mc_status"]     = 0
				computer_dict_all[computer]["mc_datetime"]   = computer_dict_mc[mc_computer]["mc_datetime"]
				computer_dict_all[computer]["mc_date"]       = computer_dict_mc[mc_computer]["mc_date"]
				computer_dict_all[computer]["mc_date_check"] = computer_dict_mc[mc_computer]["mc_date_check"]
				#
				break



	mc_conn.close()
	
#
# get sccm data and add to master dictionary
#
def get_computers_sccm():
	# name, user, site, sccm_os, sccm_sp, compliance, sc_hw_datetime, sc_hw_date, sc_sw_datetime, 
	# 0     1     2     3        4        5           6               7           8               
	#
	# sc_sw_date, sc_hw_score, sc_sw_score, sc_comp_score, sc_serial_number
	# 9           10           11           12             13
	print "\n\nQuerying SCCM....\n\n"
	# reference global variables
	global computer_dict_all
	
	# connect to sccm server
	sc_conn = pymssql.connect( server=SCCM_HOST, port=1433, user=SCCM_USER, password=SCCM_PASS, database=SCCM_DB)
	sc_conn
	cursor = sc_conn.cursor()
	
	# execute query
	cursor.execute(" \
	DECLARE @AuthListID nvarchar(300) \
	DECLARE @StateID int \
	DECLARE @StateName nvarchar(50) \
	DECLARE @CI_ID int \
	DECLARE @OSType varchar(25) \
	DECLARE @CollectionName nvarchar(255) \
	SET @OSType = 'Workstation' \
	SET @CollectionName = 'All Systems' \
	SELECT DISTINCT \
	CS.Name0, \
	CS.UserName0, \
	SYS.AD_Site_Name0, \
	SYS.Operating_System_Name_and0, \
	SYS.operatingSystemServicePac0, \
	CASE \
	WHEN (sum(case when UCS.status=2 then 1 else 0 end))>0 then ('Non-compliant') \
	ELSE 'Compliant' \
	END AS 'Status', \
	CASE \
	WHEN (sum(case when UCS.status=2 then 1 else 0 end))>0 then ((cast(sum(case when UCS.status=2 then 1 else 0 end)as int))) \
	ELSE '0' \
	END AS 'Patch Count', \
	SEU.SerialNumber0, \
	WS.LastHWScan, \
	SWSCAN.LastScanDate \
	FROM \
	v_Update_ComplianceStatITVll UCS \
	left outer join v_GS_COMPUTER_SYSTEM CS on (CS.ResourceID = UCS.ResourceID) \
	join v_UpdateInfo UI on UI.CI_ID=UCS.CI_ID \
	join v_CICategories_All CIC on CIC.CI_ID=UCS.CI_ID \
	join v_CategoryInfo CI on CIC.CategoryInstance_UniqueID = CI.CategoryInstance_UniqueID \
	left join v_FullCollectionMembership FCM on (FCM.ResourceID = CS.ResourceID) \
	left join v_R_System SYS on (SYS.ResourceID = CS.ResourceID) \
	left join v_GS_SYSTEM_ENCLOSURE_UNIQUE SEU on (SYS.ResourceID = SEU.ResourceID) \
	left join v_GS_LastSoftwareScan SWSCAN on (SYS.ResourceID = SWSCAN.ResourceID) \
	left join v_GS_WORKSTATION_STATUS WS on (WS.ResourceID = CS.ResourceID) \
	join v_Collections COL on (COL.SiteID = FCM.CollectionID) \
	left join v_ServiceWindow SW on (SW.CollectionID = COL.SiteID) \
	WHERE \
	UI.IsDeployed = '1' AND \
	COL.CollectionName in (@CollectionName) AND \
	CI.CategoryTypeName = 'Company' AND \
	SYS.Operating_System_Name_and0 like ('%' + @OSType + '%') \
	GROUP BY \
	CS.Name0, \
	CS.UserName0, \
	SYS.AD_Site_Name0, \
	SYS.Operating_System_Name_and0, \
	SYS.operatingSystemServicePac0, \
	SW.IsEnabled, \
	SEU.SerialNumber0, \
	WS.LastHWScan, \
	SWSCAN.LastScanDate \
	ORDER BY \
	CS.Name0 \
	")
	
	for row in cursor.fetchall():
		sc_computer = str(row[0])
		
		# clear all previous strings
		computer_dict_sc[sc_computer] = { \
		"sc_cn": str(row[0]), \
		"sc_user": "", \
		"sc_site": "", \
		"sc_os": "", \
		"sc_sp": "", \
		"sc_compliance": "", \
		"sc_hw_datetime": "", \
		"sc_sw_datetime": "", \
		"sc_serial_number": "", \
		"sc_missing_patches": "unknown" \
		}
		
		# read new values if present
		computer_dict_sc[sc_computer] = { \
		"sc_cn": str(row[0]), \
		"sc_user": str(row[1]), \
		"sc_site": str(row[2]), \
		"sc_os": str(row[3]), \
		"sc_sp": str(row[4]), \
		"sc_compliance": str(row[5]), \
		"sc_missing_patches": str(row[6]), \
		"sc_serial_number": str(row[7]), \
		"sc_hw_datetime": str(row[8]), \
		"sc_sw_datetime": str(row[9]) \
		}

		# create hw date field and date check
		try: 
			computer_dict_sc[sc_computer]["sc_hw_date"]      = computer_dict_sc[sc_computer]["sc_hw_datetime"].split(" ", 1)[0]
			# perform date check
			date_obj = datetime.datetime.strptime(computer_dict_sc[sc_computer]["sc_hw_date"], '%Y-%m-%d')
			if date_obj.date() > now_minus_30_days:
				computer_dict_sc[sc_computer]["sc_hw_date_check"] = 0
			else:
				computer_dict_sc[sc_computer]["sc_hw_date_check"] = 1
				
		except:
			computer_dict_sc[sc_computer]["sc_hw_date"]      = ""
			computer_dict_sc[sc_computer]["sc_hw_date_check"]     = 1
			
		# create sw date field and date check
		try: 
			computer_dict_sc[sc_computer]["sc_sw_date"]     = computer_dict_sc[sc_computer]["sc_sw_datetime"].split(" ", 1)[0]
			# perform date check
			date_obj = datetime.datetime.strptime(computer_dict_sc[sc_computer]["sc_sw_date"], '%Y-%m-%d')
			if date_obj.date() > now_minus_30_days:
				computer_dict_sc[sc_computer]["sc_sw_date_check"] = 0
			else:
				computer_dict_sc[sc_computer]["sc_sw_date_check"] = 1
		except:
			computer_dict_sc[sc_computer]["sc_sw_date"]      = ""
			computer_dict_sc[sc_computer]["sc_sw_date_check"]     = 1
		
		# create compliance score
		if computer_dict_sc[sc_computer]["sc_compliance"] == "Compliant":
			computer_dict_sc[sc_computer]["sc_comp_check"] = 0
		else:
			computer_dict_sc[sc_computer]["sc_comp_check"] = 1
			
		# check and append to master dictionary
		for computer in computer_dict_all:
			if computer_dict_all[computer]["cn"] == computer_dict_sc[sc_computer]["sc_cn"]:
				computer_dict_all[computer]["sc_status"]          = 0
				computer_dict_all[computer]["sc_hw_datetime"]     = computer_dict_sc[sc_computer]["sc_hw_datetime"]
				computer_dict_all[computer]["sc_hw_date"]         = computer_dict_sc[sc_computer]["sc_hw_date"]
				computer_dict_all[computer]["sc_hw_date_check"]   = computer_dict_sc[sc_computer]["sc_hw_date_check"]
				computer_dict_all[computer]["sc_sw_datetime"]     = computer_dict_sc[sc_computer]["sc_sw_datetime"]
				computer_dict_all[computer]["sc_sw_date"]         = computer_dict_sc[sc_computer]["sc_sw_date"]
				computer_dict_all[computer]["sc_sw_date_check"]   = computer_dict_sc[sc_computer]["sc_sw_date_check"]
				computer_dict_all[computer]["sc_compliance"]      = computer_dict_sc[sc_computer]["sc_compliance"]
				computer_dict_all[computer]["sc_comp_check"]      = computer_dict_sc[sc_computer]["sc_comp_check"]
				computer_dict_all[computer]["sc_serial_number"]   = computer_dict_sc[sc_computer]["sc_serial_number"]
				computer_dict_all[computer]["sc_site"]            = computer_dict_sc[sc_computer]["sc_site"]
				computer_dict_all[computer]["sc_user"]            = computer_dict_sc[sc_computer]["sc_user"]
				computer_dict_all[computer]["sc_os"]              = computer_dict_sc[sc_computer]["sc_os"]
				computer_dict_all[computer]["sc_sp"]              = computer_dict_sc[sc_computer]["sc_sp"]
				computer_dict_all[computer]["sc_missing_patches"] = int(computer_dict_sc[sc_computer]["sc_missing_patches"])  # integer patch count
				#
				break



#
# AGGREGATE SCORES

# aggregate scores for cb, mc and sc across ou and region
#
def ou_region_aggregate(computer_dict):
	# define global dictionary
	print "\n\nCalculating Aggregate Scores....\n\n"
	
	global ou_scores_dict
	global region_scores_dict

	for computer in computer_dict:
		ou     = computer_dict[computer]["ou"]
		region = computer_dict[computer]["region"]
		

		# ag_acore counts number of failed categories for this node
		# patch_score checks for > 50 patches missing - assume zero to start
		ag_score = 0
		patch_score = 0
		
		#### carbon black scores
			
		# check for any cb failure
		if (computer_dict[computer]["cb_status"] == 1) or (computer_dict[computer]["cb_date_check"] == 1):
			region_scores_dict[region]["cb_score"] += 1
			ou_scores_dict[ou]["cb_score"]        += 1
			ag_score += 1
			
		# print('1: ag_score after cb = ' , ag_score)
		
		#### mcafee scores
		
		# check for any mc failure
		if (computer_dict[computer]["mc_status"] == 1) or (computer_dict[computer]["mc_date_check"] == 1):
			ou_scores_dict[ou]["mc_score"]        += 1
			region_scores_dict[region]["mc_score"] += 1
			ag_score += 1
			
		
		#### sccm scores
		
	
		if computer_dict[computer]["sc_comp_check"] == 1:
			ou_scores_dict[ou]["not_fully_patched"]        += 1
			region_scores_dict[region]["not_fully_patched"] += 1
		else:
			ou_scores_dict[ou]["fully_patched"]        += 1
			region_scores_dict[region]["fully_patched"] += 1
			
		# sccm missing patch count
		
		if computer_dict[computer]["sc_missing_patches"] == "unknown":
			ou_scores_dict[ou]["unknown_patches"]         += 1
			region_scores_dict[region]["unknown_patches"] += 1
		
		try:
			patch_number = int(computer_dict[computer]["sc_missing_patches"])
		except:
			pass
		
		try:
			if patch_number > 50:
				patch_score = 1
			else:
				patch_score = 0
		except:
			patch_score = 0
			
		try:
			if (patch_number > 0) and (patch_number < 11):
				ou_scores_dict[ou]["1_10_missing"]         += 1
				region_scores_dict[region]["1_10_missing"] += 1
			elif (patch_number > 10) and (patch_number < 21):
				ou_scores_dict[ou]["11_20_missing"]         += 1
				region_scores_dict[region]["11_20_missing"] += 1
			elif (patch_number > 20) and (patch_number < 31):
				ou_scores_dict[ou]["21_30_missing"]         += 1
				region_scores_dict[region]["21_30_missing"] += 1
			elif (patch_number > 30) and (patch_number < 41):
				ou_scores_dict[ou]["31_40_missing"]         += 1
				region_scores_dict[region]["31_40_missing"] += 1
			elif (patch_number > 40) and (patch_number < 51):
				ou_scores_dict[ou]["41_50_missing"]         += 1
				region_scores_dict[region]["41_50_missing"] += 1
			elif (patch_number > 50) and (patch_number < 101):
				ou_scores_dict[ou]["51_100_missing"]         += 1
				region_scores_dict[region]["51_100_missing"] += 1
			elif (patch_number > 100):
				ou_scores_dict[ou]["over_100_missing"]         += 1
				region_scores_dict[region]["over_100_missing"] += 1
		except:
			pass
		
		# check for any sc failure
		if (computer_dict[computer]["sc_status"] == 1) or (computer_dict[computer]["sc_hw_date_check"] == 1) or (computer_dict[computer]["sc_sw_date_check"] == 1) or (patch_score == 1):
			ag_score += 1
			ou_scores_dict[ou]["sc_score"]        += 1
			region_scores_dict[region]["sc_score"] += 1



		#### bitlocker scores
		if computer_dict[computer]["bl_status"] == 1:
			ou_scores_dict[ou]["bl_score"]        += 1
			region_scores_dict[region]["bl_score"] += 1
			ag_score += 1

		#
		# count zero failures
		if ag_score == 0 :
			ou_scores_dict[ou]["0_cat"] += 1
			region_scores_dict[region]["0_cat"] += 1
	
		# count single failures
		if ag_score == 1 :
			ou_scores_dict[ou]["1_cat"] += 1
			region_scores_dict[region]["1_cat"] += 1

		# count double failures
		if ag_score == 2 :
			ou_scores_dict[ou]["2_cat"] += 1
			region_scores_dict[region]["2_cat"] += 1

		# count triple failures
		if ag_score == 3 :
			ou_scores_dict[ou]["3_cat"] += 1
			region_scores_dict[region]["3_cat"] += 1
		
		# count quadruple failures
		if ag_score == 4 :
			ou_scores_dict[ou]["4_cat"] += 1
			region_scores_dict[region]["4_cat"] += 1
		

def write_xlsx(computer_dict_all):

	print "\n\nCreating Spreadsheet....\n\n"
	# change directory
	os.chdir ('/opt/computer_state')
	
	# create a spreadsheet of results	
	workbook = xlsxwriter.Workbook('computer_state.xlsx')
	
	#####################################################
	# create first page
	# page 1 computer data
	
	worksheet = workbook.add_worksheet('computer_data')
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
	worksheet.write('A1',  'Name', bold_rotate)
	worksheet.write('B1',  'Description', bold_rotate)
	worksheet.write('C1',  'SerialNumber', bold_rotate)
	worksheet.write('D1',  'Hardware', bold_rotate)
	worksheet.write('E1',  'AD Status', bold_rotate)
	worksheet.write('F1',  'BL Status', bold_rotate)
	worksheet.write('G1',  'CB Status', bold_rotate)
	worksheet.write('H1',  'CB Date Check', bold_rotate)
	worksheet.write('I1',  'McAfee Status', bold_rotate)
	worksheet.write('J1',  'McAfee Date_Check', bold_rotate)
	worksheet.write('K1',  'SC Status', bold_rotate)
	worksheet.write('L1',  'SC HW Date Check', bold_rotate)
	worksheet.write('M1',  'SC SW Date Check', bold_rotate)
	worksheet.write('N1',  'SC Compliance Check', bold_rotate)
	worksheet.write('O1', 'SCCM Patches Missing', bold_rotate)
	worksheet.write('P1',  'OU', bold_rotate)
	worksheet.write('Q1',  'Region', bold_rotate)
	worksheet.write('R1',  'CB Sensor ID', bold_rotate)
	worksheet.write('S1',  'CB Sensor Group', bold_rotate)
	worksheet.write('T1',  'CB Operating System', bold_rotate)
	worksheet.write('U1',  'CB Last Update Time', bold_rotate)
	worksheet.write('V1',  'CB Last Update Date', bold_rotate)
	worksheet.write('W1',  'McAfee Last Update Time', bold_rotate)
	worksheet.write('X1',  'McAfee Last Update Date', bold_rotate)
	worksheet.write('Y1',  'SCCM Main User', bold_rotate)
	worksheet.write('Z1',  'SCCM Site', bold_rotate)
	worksheet.write('AA1',  'SCCM Operating System', bold_rotate)
	worksheet.write('AB1',  'SCCM Service Pack', bold_rotate)
	worksheet.write('AC1',  'SCCM Compliance', bold_rotate)
	worksheet.write('AD1', 'SCCM Hardware Check Datetime', bold_rotate)
	worksheet.write('AE1', 'SCCM Hardware Check Date', bold_rotate)
	worksheet.write('AF1', 'SCCM Software Check Datetime', bold_rotate)
	worksheet.write('AG1', 'SCCM Software Check Date', bold_rotate)

	
	 # Start from the first cell below the headers.
	row = 1
	col = 0
	
	#for name, ad_status, cb_status, cb_date_check, mc_status, mc_date_check, sc_status, \
	#sc_hw_score, sc_sw_score, sc_comp_score, ou, region, cb_id, cb_group, cb_os, cb_datetime, \
	#cb_date, mc_datetime, mc_date, sc_user, sc_site, sc_os, sc_sp, sc_compliance, sc_hw_datetime, \
	#sc_hw_date, sc_sw_datetime, sc_sw_date, hardware, bl_status, ad_description, sc_serial_number in (computer_list_ad_cb_mc_sc):
	
	for computer in computer_dict_all:
	
		worksheet.write (row, col,      computer_dict_all[computer]["cn"]   )
		worksheet.write (row, col + 1,  computer_dict_all[computer]["desc"] )
		worksheet.write (row, col + 2,  computer_dict_all[computer]["sc_serial_number"]  )
		worksheet.write (row, col + 3,  computer_dict_all[computer]["hardware"]          )
		worksheet.write (row, col + 4,  computer_dict_all[computer]["ad_status"]         )
		worksheet.write (row, col + 5,  computer_dict_all[computer]["bl_status"]         )
		worksheet.write (row, col + 6,  computer_dict_all[computer]["cb_status"]         )
		worksheet.write (row, col + 7,  computer_dict_all[computer]["cb_date_check"]     )
		worksheet.write (row, col + 8,  computer_dict_all[computer]["mc_status"]         )
		worksheet.write (row, col + 9,  computer_dict_all[computer]["mc_date_check"]     )
		worksheet.write (row, col + 10, computer_dict_all[computer]["sc_status"]         )
		worksheet.write (row, col + 11, computer_dict_all[computer]["sc_hw_date_check"]  )
		worksheet.write (row, col + 12, computer_dict_all[computer]["sc_sw_date_check"]  )
		worksheet.write (row, col + 13, computer_dict_all[computer]["sc_comp_check"]     )
		worksheet.write (row, col + 14, computer_dict_all[computer]["sc_missing_patches"])
		worksheet.write (row, col + 15, computer_dict_all[computer]["ou"]                )
		worksheet.write (row, col + 16, computer_dict_all[computer]["region"]            )
		worksheet.write (row, col + 17, computer_dict_all[computer]["cb_id"]             )
		worksheet.write (row, col + 18, computer_dict_all[computer]["cb_group"]          )
		worksheet.write (row, col + 19, computer_dict_all[computer]["cb_os"]             )
		worksheet.write (row, col + 20, computer_dict_all[computer]["cb_datetime"]       )
		worksheet.write (row, col + 21, computer_dict_all[computer]["cb_date"]           )
		worksheet.write (row, col + 22, computer_dict_all[computer]["mc_datetime"]       )
		worksheet.write (row, col + 23, computer_dict_all[computer]["mc_date"]           )
		worksheet.write (row, col + 24, computer_dict_all[computer]["sc_user"]           )
		worksheet.write (row, col + 25, computer_dict_all[computer]["sc_site"]           )
		worksheet.write (row, col + 26, computer_dict_all[computer]["sc_os"]             )
		worksheet.write (row, col + 27, computer_dict_all[computer]["sc_sp"]             )
		worksheet.write (row, col + 28, computer_dict_all[computer]["sc_compliance"]     )
		worksheet.write (row, col + 29, computer_dict_all[computer]["sc_hw_datetime"]    )
		worksheet.write (row, col + 30, computer_dict_all[computer]["sc_hw_date"]        )
		worksheet.write (row, col + 31, computer_dict_all[computer]["sc_sw_datetime"]    )
		worksheet.write (row, col + 32, computer_dict_all[computer]["sc_sw_date"]        )

		# write next row
		row += 1
	
	# insert filters on columns
	worksheet.autofilter(0, 0, row, 32)
	
	# calculate sums on scores
	Formula1 = '=SUM(F1:F{!s})'.format(row)
	Formula2 = '=SUM(G1:G{!s})'.format(row)
	Formula3 = '=SUM(H1:H{!s})'.format(row)
	Formula4 = '=SUM(I1:I{!s})'.format(row)
	Formula5 = '=SUM(J1:J{!s})'.format(row)
	Formula6 = '=SUM(K1:K{!s})'.format(row)
	Formula7 = '=SUM(L1:L{!s})'.format(row)
	Formula8 = '=SUM(M1:M{!s})'.format(row)
	Formula9 = '=SUM(N1:N{!s})'.format(row)
	
	# Add totals
	worksheet.write (row, 0, 'TOTALS', bold)
	worksheet.write (row, 5, Formula1, bold)
	worksheet.write (row, 6, Formula2, bold)
	worksheet.write (row, 7, Formula3, bold)
	worksheet.write (row, 8, Formula4, bold)
	worksheet.write (row, 9, Formula5, bold)
	worksheet.write (row, 10, Formula6, bold)
	worksheet.write (row, 11, Formula7, bold)
	worksheet.write (row, 12, Formula8, bold)
	worksheet.write (row, 13, Formula9, bold)
	
	#####################################################
	# Insert extra page
	# page 1.5 computers with problems
	
	worksheet = workbook.add_worksheet('problem_hosts')
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
	worksheet.write('A1',  'Name', bold_rotate)
	worksheet.write('B1',  'Description', bold_rotate)
	worksheet.write('C1',  'SerialNumber', bold_rotate)
	worksheet.write('D1',  'Hardware', bold_rotate)
	worksheet.write('E1',  'AD Status', bold_rotate)
	worksheet.write('F1',  'BL Status', bold_rotate)
	worksheet.write('G1',  'CB Status', bold_rotate)
	worksheet.write('H1',  'CB Date Check', bold_rotate)
	worksheet.write('I1',  'McAfee Status', bold_rotate)
	worksheet.write('J1',  'McAfee Date_Check', bold_rotate)
	worksheet.write('K1',  'SC Status', bold_rotate)
	worksheet.write('L1',  'SC HW Date Check', bold_rotate)
	worksheet.write('M1',  'SC SW Date Check', bold_rotate)
	worksheet.write('N1',  'SC Compliance Check', bold_rotate)
	worksheet.write('O1', 'SCCM Patches Missing', bold_rotate)
	worksheet.write('P1',  'OU', bold_rotate)
	worksheet.write('Q1',  'Region', bold_rotate)
	worksheet.write('R1',  'CB Sensor ID', bold_rotate)
	worksheet.write('S1',  'CB Sensor Group', bold_rotate)
	worksheet.write('T1',  'CB Operating System', bold_rotate)
	worksheet.write('U1',  'CB Last Update Time', bold_rotate)
	worksheet.write('V1',  'CB Last Update Date', bold_rotate)
	worksheet.write('W1',  'McAfee Last Update Time', bold_rotate)
	worksheet.write('X1',  'McAfee Last Update Date', bold_rotate)
	worksheet.write('Y1',  'SCCM Main User', bold_rotate)
	worksheet.write('Z1',  'SCCM Site', bold_rotate)
	worksheet.write('AA1',  'SCCM Operating System', bold_rotate)
	worksheet.write('AB1',  'SCCM Service Pack', bold_rotate)
	worksheet.write('AC1',  'SCCM Compliance', bold_rotate)
	worksheet.write('AD1', 'SCCM Hardware Check Datetime', bold_rotate)
	worksheet.write('AE1', 'SCCM Hardware Check Date', bold_rotate)
	worksheet.write('AF1', 'SCCM Software Check Datetime', bold_rotate)
	worksheet.write('AG1', 'SCCM Software Check Date', bold_rotate)

	
	 # Start from the first cell below the headers.
	row = 1
	col = 0
	
	#for name, ad_status, cb_status, cb_date_check, mc_status, mc_date_check, sc_status, \
	#sc_hw_score, sc_sw_score, sc_comp_score, ou, region, cb_id, cb_group, cb_os, cb_datetime, \
	#cb_date, mc_datetime, mc_date, sc_user, sc_site, sc_os, sc_sp, sc_compliance, sc_hw_datetime, \
	#sc_hw_date, sc_sw_datetime, sc_sw_date, hardware, bl_status, ad_description, sc_serial_number in (computer_list_ad_cb_mc_sc):
	
	for computer in computer_dict_all:
		
		comp_status = \
		int(computer_dict_all[computer]["bl_status"]) + \
		int(computer_dict_all[computer]["cb_status"])  + \
		int(computer_dict_all[computer]["cb_date_check"]) + \
		int(computer_dict_all[computer]["mc_status"]) + \
		int(computer_dict_all[computer]["mc_date_check"]) + \
		int(computer_dict_all[computer]["sc_status"]) + \
		int(computer_dict_all[computer]["sc_hw_date_check"]) + \
		int(computer_dict_all[computer]["sc_sw_date_check"])+ \
		int(computer_dict_all[computer]["sc_comp_check"]) 
				
		if comp_status > 0:
			worksheet.write (row, col,      computer_dict_all[computer]["cn"]   )
			worksheet.write (row, col + 1,  computer_dict_all[computer]["desc"] )
			worksheet.write (row, col + 2,  computer_dict_all[computer]["sc_serial_number"]  )
			worksheet.write (row, col + 3,  computer_dict_all[computer]["hardware"]          )
			worksheet.write (row, col + 4,  computer_dict_all[computer]["ad_status"]         )
			worksheet.write (row, col + 5,  computer_dict_all[computer]["bl_status"]         )
			worksheet.write (row, col + 6,  computer_dict_all[computer]["cb_status"]         )
			worksheet.write (row, col + 7,  computer_dict_all[computer]["cb_date_check"]     )
			worksheet.write (row, col + 8,  computer_dict_all[computer]["mc_status"]         )
			worksheet.write (row, col + 9,  computer_dict_all[computer]["mc_date_check"]     )
			worksheet.write (row, col + 10, computer_dict_all[computer]["sc_status"]         )
			worksheet.write (row, col + 11, computer_dict_all[computer]["sc_hw_date_check"]  )
			worksheet.write (row, col + 12, computer_dict_all[computer]["sc_sw_date_check"]  )
			worksheet.write (row, col + 13, computer_dict_all[computer]["sc_comp_check"]     )
			worksheet.write (row, col + 14, computer_dict_all[computer]["sc_missing_patches"])
			worksheet.write (row, col + 15, computer_dict_all[computer]["ou"]                )
			worksheet.write (row, col + 16, computer_dict_all[computer]["region"]            )
			worksheet.write (row, col + 17, computer_dict_all[computer]["cb_id"]             )
			worksheet.write (row, col + 18, computer_dict_all[computer]["cb_group"]          )
			worksheet.write (row, col + 19, computer_dict_all[computer]["cb_os"]             )
			worksheet.write (row, col + 20, computer_dict_all[computer]["cb_datetime"]       )
			worksheet.write (row, col + 21, computer_dict_all[computer]["cb_date"]           )
			worksheet.write (row, col + 22, computer_dict_all[computer]["mc_datetime"]       )
			worksheet.write (row, col + 23, computer_dict_all[computer]["mc_date"]           )
			worksheet.write (row, col + 24, computer_dict_all[computer]["sc_user"]           )
			worksheet.write (row, col + 25, computer_dict_all[computer]["sc_site"]           )
			worksheet.write (row, col + 26, computer_dict_all[computer]["sc_os"]             )
			worksheet.write (row, col + 27, computer_dict_all[computer]["sc_sp"]             )
			worksheet.write (row, col + 28, computer_dict_all[computer]["sc_compliance"]     )
			worksheet.write (row, col + 29, computer_dict_all[computer]["sc_hw_datetime"]    )
			worksheet.write (row, col + 30, computer_dict_all[computer]["sc_hw_date"]        )
			worksheet.write (row, col + 31, computer_dict_all[computer]["sc_sw_datetime"]    )
			worksheet.write (row, col + 32, computer_dict_all[computer]["sc_sw_date"]        )

		# write next row
			row += 1
	
	# insert filters on columns
	worksheet.autofilter(0, 0, row, 32)
	
	# calculate sums on scores
	Formula1 = '=SUM(F1:F{!s})'.format(row)
	Formula2 = '=SUM(G1:G{!s})'.format(row)
	Formula3 = '=SUM(H1:H{!s})'.format(row)
	Formula4 = '=SUM(I1:I{!s})'.format(row)
	Formula5 = '=SUM(J1:J{!s})'.format(row)
	Formula6 = '=SUM(K1:K{!s})'.format(row)
	Formula7 = '=SUM(L1:L{!s})'.format(row)
	Formula8 = '=SUM(M1:M{!s})'.format(row)
	Formula9 = '=SUM(N1:N{!s})'.format(row)
	
	# Add totals
	worksheet.write (row, 0, 'TOTALS', bold)
	worksheet.write (row, 5, Formula1, bold)
	worksheet.write (row, 6, Formula2, bold)
	worksheet.write (row, 7, Formula3, bold)
	worksheet.write (row, 8, Formula4, bold)
	worksheet.write (row, 9, Formula5, bold)
	worksheet.write (row, 10, Formula6, bold)
	worksheet.write (row, 11, Formula7, bold)
	worksheet.write (row, 12, Formula8, bold)
	worksheet.write (row, 13, Formula9, bold)
	
	
	########################################################################################
	# page 2
	# create second page for ou counts
	#
	worksheet = workbook.add_worksheet('ou_data')
	worksheet.freeze_panes(1, 0)
	
	# write headers
	worksheet.write('A1', 'OU', bold_rotate)
	worksheet.write('B1', 'Region', bold_rotate)
	worksheet.write('C1', 'Carbon Black Failures', bold_rotate)
	worksheet.write('D1', 'McAfee Failures', bold_rotate)
	worksheet.write('E1', 'SCCM Failures', bold_rotate)
	worksheet.write('F1', 'Bitlocker Failures', bold_rotate)
	worksheet.write('G1', 'Zero Failures', bold_rotate)
	worksheet.write('H1', 'One Category', bold_rotate)
	worksheet.write('I1', 'Two Categories', bold_rotate)
	worksheet.write('J1', 'Three Categories', bold_rotate)
	worksheet.write('K1', 'Four Categories', bold_rotate)
	worksheet.write('L1', 'Fully Patched', bold_rotate)
	worksheet.write('M1', '1 to 10 Missing', bold_rotate)
	worksheet.write('N1', '11 to 20 Missing', bold_rotate)
	worksheet.write('O1', '21 to 30 Missing', bold_rotate)
	worksheet.write('P1', '31 to 40 Missing', bold_rotate)
	worksheet.write('Q1', '41 to 50 Missing', bold_rotate)
	worksheet.write('R1', '51 to 100 Missing', bold_rotate)
	worksheet.write('S1', 'Over 100 Missing', bold_rotate)
	worksheet.write('T1', 'Patches Unknown', bold_rotate)
	
	# Start from the first cell below the headers.
	row = 1
	col = 0
	
	# write aggregate scores for each ou
	# for ou, ou_stats in ou_scores_dict.iteritems():
	
	for ou in ou_scores_dict:
	
		worksheet.write (row, col,       ou_scores_dict[ou]["ou"]                )
		worksheet.write (row, col + 1,   ou_scores_dict[ou]["region"]            )
		worksheet.write (row, col + 2,   ou_scores_dict[ou]["cb_score"]          )
		worksheet.write (row, col + 3,   ou_scores_dict[ou]["mc_score"]          )
		worksheet.write (row, col + 4,   ou_scores_dict[ou]["sc_score"]          )
		worksheet.write (row, col + 5,   ou_scores_dict[ou]["bl_score"]          )
		worksheet.write (row, col + 6,   ou_scores_dict[ou]["0_cat"]             )
		worksheet.write (row, col + 7,   ou_scores_dict[ou]["1_cat"]             )
		worksheet.write (row, col + 8,   ou_scores_dict[ou]["2_cat"]             )
		worksheet.write (row, col + 9,   ou_scores_dict[ou]["3_cat"]             )
		worksheet.write (row, col + 10,  ou_scores_dict[ou]["4_cat"]             )
		worksheet.write (row, col + 11,  ou_scores_dict[ou]["fully_patched"]     )
		worksheet.write (row, col + 12,  ou_scores_dict[ou]["1_10_missing"]      )
		worksheet.write (row, col + 13,  ou_scores_dict[ou]["11_20_missing"]     )
		worksheet.write (row, col + 14,  ou_scores_dict[ou]["21_30_missing"]     )
		worksheet.write (row, col + 15,  ou_scores_dict[ou]["31_40_missing"]     )
		worksheet.write (row, col + 16,  ou_scores_dict[ou]["41_50_missing"]     )
		worksheet.write (row, col + 17,  ou_scores_dict[ou]["51_100_missing"]    )
		worksheet.write (row, col + 18,  ou_scores_dict[ou]["over_100_missing"]  )
		worksheet.write (row, col + 19,  ou_scores_dict[ou]["unknown_patches"]  )
		row += 1
		col  = 0
	
	# insert filters on columns
	worksheet.autofilter(0, 0, row, 19)
	
	# calculate sums on scores
	Formula1  = '=SUM(C1:C{!s})'.format(row)
	Formula2  = '=SUM(D1:D{!s})'.format(row)
	Formula3  = '=SUM(E1:E{!s})'.format(row)
	Formula4  = '=SUM(F1:F{!s})'.format(row)
	Formula5  = '=SUM(G1:G{!s})'.format(row)
	Formula6  = '=SUM(H1:H{!s})'.format(row)
	Formula7  = '=SUM(I1:I{!s})'.format(row)
	Formula8  = '=SUM(J1:J{!s})'.format(row)
	Formula9  = '=SUM(K1:K{!s})'.format(row)
	Formula10 = '=SUM(L1:L{!s})'.format(row)
	Formula11 = '=SUM(M1:M{!s})'.format(row)
	Formula12 = '=SUM(N1:N{!s})'.format(row)
	Formula13 = '=SUM(O1:O{!s})'.format(row)
	Formula14 = '=SUM(P1:P{!s})'.format(row)
	Formula15 = '=SUM(Q1:Q{!s})'.format(row)
	Formula16 = '=SUM(R1:R{!s})'.format(row)
	Formula17 = '=SUM(S1:S{!s})'.format(row)
	Formula18 = '=SUM(T1:T{!s})'.format(row)
	
	# Add totals
	worksheet.write (row, 0,  'TOTALS',  bold)
	worksheet.write (row, 2,  Formula1,  bold)
	worksheet.write (row, 3,  Formula2,  bold)
	worksheet.write (row, 4,  Formula3,  bold)
	worksheet.write (row, 5,  Formula4,  bold)
	worksheet.write (row, 6,  Formula5,  bold)
	worksheet.write (row, 7,  Formula6,  bold)
	worksheet.write (row, 8,  Formula7,  bold)
	worksheet.write (row, 9,  Formula8,  bold)
	worksheet.write (row, 10, Formula9,  bold)
	worksheet.write (row, 11, Formula10, bold)
	worksheet.write (row, 12, Formula11, bold)
	worksheet.write (row, 13, Formula12, bold)
	worksheet.write (row, 14, Formula13, bold)
	worksheet.write (row, 15, Formula14, bold)
	worksheet.write (row, 16, Formula15, bold)
	worksheet.write (row, 17, Formula16, bold)
	worksheet.write (row, 18, Formula17, bold)
	worksheet.write (row, 19, Formula18, bold)
	
	########################################################################################
	# page 3
	# create third page for region counts
	#
	worksheet = workbook.add_worksheet('region_data')
	worksheet.freeze_panes(1, 0)
	
	# write headers
	worksheet.write('A1', 'Region', bold_rotate)
	worksheet.write('B1', 'Carbon Black Failures', bold_rotate)
	worksheet.write('C1', 'McAfee Failures', bold_rotate)
	worksheet.write('D1', 'SCCM Failures', bold_rotate)
	worksheet.write('E1', 'Bitlocker Failures', bold_rotate)
	worksheet.write('F1', 'Working Properly', bold_rotate)
	worksheet.write('G1', 'One Category', bold_rotate)
	worksheet.write('H1', 'Two Categories', bold_rotate)
	worksheet.write('I1', 'Three Categories', bold_rotate)
	worksheet.write('J1', 'Four Categories', bold_rotate)
	worksheet.write('K1', 'Fully Patched', bold_rotate)
	worksheet.write('L1', '1 to 10 Missing', bold_rotate)
	worksheet.write('M1', '11 to 20 Missing', bold_rotate)
	worksheet.write('N1', '21 to 30 Missing', bold_rotate)
	worksheet.write('O1', '31 to 40 Missing', bold_rotate)
	worksheet.write('P1', '41 to 50 Missing', bold_rotate)
	worksheet.write('Q1', '51 to 100 Missing', bold_rotate)
	worksheet.write('R1', 'Over 100 Missing', bold_rotate)
	worksheet.write('S1', 'Patches Unknown', bold_rotate)
	
	# Start from the first cell below the headers.
	row = 1
	col = 0
	
	# write aggregate scores for each region
	#for region, region_stats in region_scores_dict.iteritems():
	
	for region in region_scores_dict:
	
		worksheet.write (row, col,      region_scores_dict[region]["region"]            )
		worksheet.write (row, col + 1,  region_scores_dict[region]["cb_score"]          )
		worksheet.write (row, col + 2,  region_scores_dict[region]["mc_score"]          )
		worksheet.write (row, col + 3,  region_scores_dict[region]["sc_score"]          )
		worksheet.write (row, col + 4,  region_scores_dict[region]["bl_score"]          )
		worksheet.write (row, col + 5,  region_scores_dict[region]["0_cat"]             )
		worksheet.write (row, col + 6,  region_scores_dict[region]["1_cat"]             )
		worksheet.write (row, col + 7,  region_scores_dict[region]["2_cat"]             )
		worksheet.write (row, col + 8,  region_scores_dict[region]["3_cat"]             )
		worksheet.write (row, col + 9,  region_scores_dict[region]["4_cat"]             )
		worksheet.write (row, col + 10, region_scores_dict[region]["fully_patched"]     )
		worksheet.write (row, col + 11,  region_scores_dict[region]["1_10_missing"]      )
		worksheet.write (row, col + 12,  region_scores_dict[region]["11_20_missing"]     )
		worksheet.write (row, col + 13,  region_scores_dict[region]["21_30_missing"]     )
		worksheet.write (row, col + 14,  region_scores_dict[region]["31_40_missing"]     )
		worksheet.write (row, col + 15,  region_scores_dict[region]["41_50_missing"]     )
		worksheet.write (row, col + 16,  region_scores_dict[region]["51_100_missing"]    )
		worksheet.write (row, col + 17,  region_scores_dict[region]["over_100_missing"]  )
		worksheet.write (row, col + 18,  region_scores_dict[region]["unknown_patches"]  )
		row += 1
		col  = 0

	# insert filters on columns
	worksheet.autofilter(0, 0, row, 18)
	
	# calculate sums on scores
	Formula1  = '=SUM(B1:B{!s})'.format(row)
	Formula2  = '=SUM(C1:C{!s})'.format(row)
	Formula3  = '=SUM(D1:D{!s})'.format(row)
	Formula4  = '=SUM(E1:E{!s})'.format(row)
	Formula5  = '=SUM(F1:F{!s})'.format(row)
	Formula6  = '=SUM(G1:G{!s})'.format(row)
	Formula7  = '=SUM(H1:H{!s})'.format(row)
	Formula8  = '=SUM(I1:I{!s})'.format(row)
	Formula9  = '=SUM(J1:J{!s})'.format(row)
	Formula10 = '=SUM(K1:K{!s})'.format(row)
	Formula11 = '=SUM(L1:L{!s})'.format(row)
	Formula12 = '=SUM(M1:M{!s})'.format(row)
	Formula13 = '=SUM(N1:N{!s})'.format(row)
	Formula14 = '=SUM(O1:O{!s})'.format(row)
	Formula15 = '=SUM(P1:P{!s})'.format(row)
	Formula16 = '=SUM(Q1:Q{!s})'.format(row)
	Formula17 = '=SUM(R1:R{!s})'.format(row)
	Formula18 = '=SUM(S1:S{!s})'.format(row)
	
	# Add totals
	worksheet.write (row, 0,  'TOTALS',  bold)
	worksheet.write (row, 1,  Formula1,  bold)
	worksheet.write (row, 2,  Formula2,  bold)
	worksheet.write (row, 3,  Formula3,  bold)
	worksheet.write (row, 4,  Formula4,  bold)
	worksheet.write (row, 5,  Formula5,  bold)
	worksheet.write (row, 6,  Formula6,  bold)
	worksheet.write (row, 7,  Formula7,  bold)
	worksheet.write (row, 8,  Formula8,  bold)
	worksheet.write (row, 9,  Formula9,  bold)
	worksheet.write (row, 10, Formula10, bold)
	worksheet.write (row, 11, Formula11, bold)
	worksheet.write (row, 12, Formula12, bold)
	worksheet.write (row, 13, Formula13, bold)
	worksheet.write (row, 14, Formula14, bold)
	worksheet.write (row, 15, Formula15, bold)
	worksheet.write (row, 16, Formula16, bold)
	worksheet.write (row, 17, Formula17, bold)
	worksheet.write (row, 18, Formula18, bold)
	
	
	##############################################################################
	#
	# define row counts for ou and region charts
	
	O = ou_count 
	R = region_count
	
	
	#########################################################################
	# page 4
	# create page with chart of aggregate ou data
	#
	# add worksheet
	worksheet = workbook.add_worksheet('ou summary')
	
	#######################################################################
	#
	# Create a new column chart.
	#
	chart1 = workbook.add_chart({'type': 'column'})
	
	
	# Configure the first series: machines which fail on cb
	chart1.add_series({
		'name':       ['ou_data', 0, 2],
		'categories': ['ou_data', 1, 0, O, 0],
		'values':     ['ou_data', 1, 2, O, 2],
	})

	# Configure a second series: machines which fail on mc
	chart1.add_series({
		'name':       ['ou_data', 0, 3],
		'categories': ['ou_data', 1, 0, O, 0],
		'values':     ['ou_data', 1, 3, O, 3],
	})
	
	# Configure a third series: machines which fail on sccm
	chart1.add_series({
		'name':       ['ou_data', 0, 4],
		'categories': ['ou_data', 1, 0, O, 0],
		'values':     ['ou_data', 1, 4, O, 4],
	})

	# Configure a fourth series: machines which fail on bitlocker
	chart1.add_series({
		'name':       ['ou_data', 0, 5],
		'categories': ['ou_data', 1, 0, O, 0],
		'values':     ['ou_data', 1, 5, O, 5],
	})
	
	# Add a chart title and some axis labels.
	chart1.set_title ({'name': 'Computer Failures by Category, OU'})
	chart1.set_x_axis({'name': 'Computer OU'})
	chart1.set_y_axis({'name': 'Failure Count'})
	chart1.set_x_axis({'num_font':  {'rotation': 270}})

	# Set an Excel chart style.
	chart1.set_style(11)
	chart1.set_plotarea({'gradient': {'colors': ['#FFEFD1', '#F0EBD5', '#B69F66']}})
	chart1.set_table()
	chart1.set_size({'x_scale': 6, 'y_scale': 3})
	
	
	# Insert the chart into the worksheet (with an offset).
	worksheet.insert_chart('B2', chart1, {'x_offset': 25, 'y_offset': 10})

	#########################################################################
	# page 5
	# create  page with chart of aggregate ou data stacked
	#
	# add worksheet
	worksheet = workbook.add_worksheet('ou summary stacked')
	
	#######################################################################
	#
	# Create a stacked chart sub-type.
	#
	chart2 = workbook.add_chart({'type': 'column', 'subtype': 'stacked'})

	# Configure the first series: machines which fail on cb
	chart2.add_series({
		'name':       ['ou_data', 0, 2],
		'categories': ['ou_data', 1, 0, O, 0],
		'values':     ['ou_data', 1, 2, O, 2],
	})

	# Configure a second series: machines which fail on mc
	chart2.add_series({
		'name':       ['ou_data', 0, 3],
		'categories': ['ou_data', 1, 0, O, 0],
		'values':     ['ou_data', 1, 3, O, 3],
	})
	
	# Configure a third series: machines which fail on sccm
	chart2.add_series({
		'name':       ['ou_data', 0, 4],
		'categories': ['ou_data', 1, 0, O, 0],
		'values':     ['ou_data', 1, 4, O, 4],
	})
	
	# Configure a fourth series: machines which fail on bitlocker
	chart2.add_series({
		'name':       ['ou_data', 0, 5],
		'categories': ['ou_data', 1, 0, O, 0],
		'values':     ['ou_data', 1, 5, O, 5],
	})
	
	# Add a chart title and some axis labels.
	chart2.set_title ({'name': 'Computer Failures by Category, OU: Stacked'})
	chart2.set_x_axis({'name': 'Computer OU'})
	chart2.set_y_axis({'name': 'Failure Count'})
	chart2.set_x_axis({'num_font':  {'rotation': 270}})


	# Set an Excel chart style.
	chart2.set_style(12)
	chart2.set_plotarea({'gradient': {'colors': ['#FFEFD1', '#F0EBD5', '#B69F66']}})
	chart2.set_table()
	chart2.set_size({'x_scale': 6, 'y_scale': 3})
	

	# Insert the chart into the worksheet (with an offset).
	worksheet.insert_chart('B2', chart2, {'x_offset': 25, 'y_offset': 10})

	
	#########################################################################
	# page 6
	# create page with chart of aggregate region data
	#
	# add worksheet
	worksheet = workbook.add_worksheet('region summary')
	
	#######################################################################
	#
	# Create a new column chart.
	#
	chart3 = workbook.add_chart({'type': 'column'})
	
	
	# Configure the first series: machines which fail on cb
	chart3.add_series({
		'name':       ['region_data', 0, 1],
		'categories': ['region_data', 1, 0, R, 0],
		'values':     ['region_data', 1, 1, R, 1],
	})

	# Configure a second series: machines which fail on mc
	chart3.add_series({
		'name':       ['region_data', 0, 2],
		'categories': ['region_data', 1, 0, R, 0],
		'values':     ['region_data', 1, 2, R, 2],
	})
	
	# Configure a third series: machines which fail on sccm
	chart3.add_series({
		'name':       ['region_data', 0, 3],
		'categories': ['region_data', 1, 0, R, 0],
		'values':     ['region_data', 1, 3, R, 3],
	})
	
	# Configure a fourth series: machines which fail on bitlocker
	chart3.add_series({
		'name':       ['region_data', 0, 4],
		'categories': ['region_data', 1, 0, R, 0],
		'values':     ['region_data', 1, 4, R, 4],
	})
	
	# Add a chart title and some axis labels.
	chart3.set_title ({'name': 'Computer Failures by Category, Region'})
	chart3.set_x_axis({'name': 'Region'})
	chart3.set_y_axis({'name': 'Failure Count'})
	chart3.set_x_axis({'num_font':  {'rotation': 270}})

	# Set an Excel chart style.
	chart3.set_style(11)
	chart3.set_plotarea({'gradient': {'colors': ['#FFEFD1', '#F0EBD5', '#B69F66']}})
	chart3.set_table()
	chart3.set_size({'x_scale': 3, 'y_scale': 3})
	
	
	# Insert the chart into the worksheet (with an offset).
	worksheet.insert_chart('B2', chart3, {'x_offset': 25, 'y_offset': 10})

	#########################################################################
	# page 7
	# create  page with chart of aggregate region data stacked
	#
	# add worksheet
	worksheet = workbook.add_worksheet('regions summary stacked')
	
	#######################################################################
	#
	# Create a stacked chart sub-type.
	#
	chart4 = workbook.add_chart({'type': 'column', 'subtype': 'stacked'})

	# Configure the first series: machines which fail on cb
	chart4.add_series({
		'name':       ['region_data', 0, 1],
		'categories': ['region_data', 1, 0, R, 0],
		'values':     ['region_data', 1, 1, R, 1],
	})

	# Configure a second series: machines which fail on mc
	chart4.add_series({
		'name':       ['region_data', 0, 2],
		'categories': ['region_data', 1, 0, R, 0],
		'values':     ['region_data', 1, 2, R, 2],
	})
	
	# Configure a third series: machines which fail on sccm
	chart4.add_series({
		'name':       ['region_data', 0, 3],
		'categories': ['region_data', 1, 0, R, 0],
		'values':     ['region_data', 1, 3, R, 3],
	})

	# Configure a fourth series: machines which fail on bitlocker
	chart4.add_series({
		'name':       ['region_data', 0, 4],
		'categories': ['region_data', 1, 0, R, 0],
		'values':     ['region_data', 1, 4, R, 4],
	})

	# Add a chart title and some axis labels.
	chart4.set_title ({'name': 'Computer Failures by Category, Region: Stacked'})
	chart4.set_x_axis({'name': 'Region'})
	chart4.set_y_axis({'name': 'Failure Count'})
	chart4.set_x_axis({'num_font':  {'rotation': 270}})


	# Set an Excel chart style.
	chart4.set_style(12)
	chart4.set_plotarea({'gradient': {'colors': ['#FFEFD1', '#F0EBD5', '#B69F66']}})
	chart4.set_table()
	chart4.set_size({'x_scale': 3, 'y_scale': 3})
	

	# Insert the chart into the worksheet (with an offset).
	worksheet.insert_chart('B2', chart4, {'x_offset': 25, 'y_offset': 10})

	#########################################################################
	# page 8
	# create page with chart of aggregate multiple failures ou data
	#
	# add worksheet
	worksheet = workbook.add_worksheet('ou multiple failures')
	
	#######################################################################
	#
	# Create a new column chart.
	#
	chart5 = workbook.add_chart({'type': 'column'})
	
	
	# Configure the first series: machines with failures in two categories
	chart5.add_series({
		'name':       ['ou_data', 0, 8],
		'categories': ['ou_data', 1, 0, O, 0],
		'values':     ['ou_data', 1, 8, O, 8],
	})

	# Configure a second series: machines with failures in three categories
	chart5.add_series({
		'name':       ['ou_data', 0, 9],
		'categories': ['ou_data', 1, 0, O, 0],
		'values':     ['ou_data', 1, 9, O, 9],
	})
	
	# Configure a third series: machines with failures in four categories
	chart5.add_series({
		'name':       ['ou_data', 0, 10],
		'categories': ['ou_data', 1, 0, O, 0],
		'values':     ['ou_data', 1, 10, O, 10],
	})
	
	
	# Add a chart title and some axis labels.
	chart5.set_title ({'name': 'Computers with Multiple Failures by Category, OU'})
	chart5.set_x_axis({'name': 'Computer OU'})
	chart5.set_y_axis({'name': 'Failing Computer Count'})
	chart5.set_x_axis({'num_font':  {'rotation': 270}})

	# Set an Excel chart style.
	chart5.set_style(11)
	chart5.set_plotarea({'gradient': {'colors': ['#FFEFD1', '#F0EBD5', '#B69F66']}})
	chart5.set_table()
	chart5.set_size({'x_scale': 6, 'y_scale': 3})
	
	
	# Insert the chart into the worksheet (with an offset).
	worksheet.insert_chart('B2', chart5, {'x_offset': 25, 'y_offset': 10})

	
	#########################################################################
	# page 9
	# create  page with chart of aggregate multiple failures ou data stacked
	#
	# add worksheet
	worksheet = workbook.add_worksheet('ou multiple failures stacked')
	
	#######################################################################
	#
	# Create a stacked chart sub-type.
	#
	chart6 = workbook.add_chart({'type': 'column', 'subtype': 'stacked'})


	# Configure a second series: machines with two failures
	chart6.add_series({
		'name':       ['ou_data', 0, 8],
		'categories': ['ou_data', 1, 0, O, 0],
		'values':     ['ou_data', 1, 8, O, 8],
	})
	
	# Configure a third series: machines with three failures
	chart6.add_series({
		'name':       ['ou_data', 0, 9],
		'categories': ['ou_data', 1, 0, O, 0],
		'values':     ['ou_data', 1, 9, O, 9],
	})
	
	# Configure a fourth series: machines with four failures
	chart6.add_series({
		'name':       ['ou_data', 0, 10],
		'categories': ['ou_data', 1, 0, O, 0],
		'values':     ['ou_data', 1, 10, O, 10],
	})
	
	
	# Add a chart title and some axis labels.
	chart6.set_title ({'name': 'Computers with Multiple Failures by Category, OU: Stacked'})
	chart6.set_x_axis({'name': 'Computer OU'})
	chart6.set_y_axis({'name': 'Failing Computer Count'})
	chart6.set_x_axis({'num_font':  {'rotation': 270}})


	# Set an Excel chart style.
	chart6.set_style(12)
	chart6.set_plotarea({'gradient': {'colors': ['#FFEFD1', '#F0EBD5', '#B69F66']}})
	chart6.set_table()
	chart6.set_size({'x_scale': 6, 'y_scale': 3})
	

	# Insert the chart into the worksheet (with an offset).
	worksheet.insert_chart('B2', chart6, {'x_offset': 25, 'y_offset': 10})

	#########################################################################
	# page 10
	# create page with chart of aggregate region multiple failures data
	#
	# add worksheet
	worksheet = workbook.add_worksheet('region multiple failures')
	
	#######################################################################
	#
	# Create a new column chart.
	#
	chart7 = workbook.add_chart({'type': 'column'})
	
	

	# Configure a second series: machines with two failures
	chart7.add_series({
		'name':       ['region_data', 0, 7],
		'categories': ['region_data', 1, 0, R, 0],
		'values':     ['region_data', 1, 7, R, 7],
	})
	
	# Configure a third series: machines with three failures
	chart7.add_series({
		'name':       ['region_data', 0, 8],
		'categories': ['region_data', 1, 0, R, 0],
		'values':     ['region_data', 1, 8, R, 8],
	})
	
	# Configure a fourth series: machines with four failures
	chart7.add_series({
		'name':       ['region_data', 0, 9],
		'categories': ['region_data', 1, 0, R, 0],
		'values':     ['region_data', 1, 9, R, 9],
	})
	
	
	# Add a chart title and some axis labels.
	chart7.set_title ({'name': 'Computers with Multiple Failures by Category, Region'})
	chart7.set_x_axis({'name': 'Region'})
	chart7.set_y_axis({'name': 'Failure Count'})
	chart7.set_x_axis({'num_font':  {'rotation': 270}})

	# Set an Excel chart style.
	chart7.set_style(11)
	chart7.set_plotarea({'gradient': {'colors': ['#FFEFD1', '#F0EBD5', '#B69F66']}})
	chart7.set_table()
	chart7.set_size({'x_scale': 3, 'y_scale': 3})
	
	
	# Insert the chart into the worksheet (with an offset).
	worksheet.insert_chart('B2', chart7, {'x_offset': 25, 'y_offset': 10})

	#########################################################################
	# page 11
	# create  page with chart of aggregate region data stacked
	#
	# add worksheet
	worksheet = workbook.add_worksheet('region multiple failure stacked')
	
	#######################################################################
	#
	# Create a stacked chart sub-type.
	#
	chart8 = workbook.add_chart({'type': 'column', 'subtype': 'stacked'})

	# Configure a second series: machines with two failures
	chart8.add_series({
		'name':       ['region_data', 0, 7],
		'categories': ['region_data', 1, 0, R, 0],
		'values':     ['region_data', 1, 7, R, 7],
	})
	
	# Configure a third series: machines with three failures
	chart8.add_series({
		'name':       ['region_data', 0, 8],
		'categories': ['region_data', 1, 0, R, 0],
		'values':     ['region_data', 1, 8, R, 8],
	})
	
	# Configure a fourth series: machines with four failures
	chart8.add_series({
		'name':       ['region_data', 0, 9],
		'categories': ['region_data', 1, 0, R, 0],
		'values':     ['region_data', 1, 9, R, 9],
	})

	# Add a chart title and some axis labels.
	chart8.set_title ({'name': 'Computers with Multiple Failures by Category, Region: Stacked'})
	chart8.set_x_axis({'name': 'Region'})
	chart8.set_y_axis({'name': 'Failure Count'})
	chart8.set_x_axis({'num_font':  {'rotation': 270}})


	# Set an Excel chart style.
	chart8.set_style(12)
	chart8.set_plotarea({'gradient': {'colors': ['#FFEFD1', '#F0EBD5', '#B69F66']}})
	chart8.set_table()
	chart8.set_size({'x_scale': 3, 'y_scale': 3})
	

	# Insert the chart into the worksheet (with an offset).
	worksheet.insert_chart('B2', chart8, {'x_offset': 25, 'y_offset': 10})
	

	#########################################################################
	# page 12
	# create page with pie chart summary 
	#
	# add worksheet
	worksheet = workbook.add_worksheet('health and failure summary')
	
	#######################################################################
	#
	# Create a pie chart for sccm failures
	chart9 = workbook.add_chart({'type': 'pie'})
	
	
	# Configure a data series
	chart9.add_series({
		'name':       'SCCM Compliance',
		'categories': ['region_data', 0, 10, 0, 18] ,
		'values':     ['region_data', (R+1), 10, (R+1), 18 ] ,
		'data_labels': {'value': True},
	#	'values':     ['region_data', (R+1), 5, (R+1), 5], ['region_data', (R+1), 10, (R+1), 10]
	})


	# Add a chart title and some axis labels.
	chart9.set_title ({'name': 'SCCM Compliance'})

	# Set an Excel chart style.
	chart9.set_style(10)

	chart9.set_size({'x_scale': 1.5, 'y_scale': 1.5})
	

	# Insert the chart into the worksheet (with an offset).
	worksheet.insert_chart('B2', chart9, {'x_offset': 25, 'y_offset': 10})
	

	######################################################################
	#
	# Create a pie chart for health.
	#
	chart10 = workbook.add_chart({'type': 'pie'})
	
	
	# Configure a data series
	chart10.add_series({
		'name':       'Computer Health Count',
		'categories': ['region_data', 0, 5, 0, 9],
		'values':     ['region_data', (R+1), 5, (R+1), 9],
		'data_labels': {'value': True},
	})


	# Add a chart title and some axis labels.
	chart10.set_title ({'name': 'Computers with Failures by Category Count'})

	# Set an Excel chart style.
	chart10.set_style(10)

	chart10.set_size({'x_scale': 1.5, 'y_scale': 1.5})
	

	# Insert the chart into the worksheet (with an offset).
	worksheet.insert_chart('B27', chart10, {'x_offset': 25, 'y_offset': 10})
	
	#######################################################################
	#
	# Create a pie chart for failure counts
	#
	chart11 = workbook.add_chart({'type': 'pie'})
	
	
	# Configure a data series
	chart11.add_series({
		'name':       'Failure count by category',
		'categories': ['region_data', 0, 1, 0, 4],
		'values':     ['region_data', (R+1), 1, (R+1), 4],
		'data_labels': {'value': True},
	})


	# Add a chart title and some axis labels.
	chart11.set_title ({'name': 'Failure Count by Category'})

	# Set an Excel chart style.
	chart11.set_style(10)

	chart11.set_size({'x_scale': 1.5, 'y_scale': 1.5})
	

	# Insert the chart into the worksheet (with an offset).
	worksheet.insert_chart('N2', chart11, {'x_offset': 25, 'y_offset': 10})
	
	#row on region_data holding sum totals
	T = (row +1)
	
	# insert totals
	worksheet.write('P27', 'SCCM Compliance Count', bold)
	worksheet.write('O28', 'Fully Patched', bold_border)
	worksheet.write('O29', '1 to 10', bold_border)
	worksheet.write('O30', '11 to 20', bold_border)
	worksheet.write('O31', '21 to 30', bold_border)
	worksheet.write('O32', '31 to 40', bold_border)
	worksheet.write('O33', '41 to 50', bold_border)
	worksheet.write('O34', '51 to 100', bold_border)
	worksheet.write('O35', 'over 100', bold_border)
	worksheet.write('O36', 'patches unknown', bold_border)
	worksheet.write('P28', '=(region_data!$K${!s}:$K${!s})'.format(T, T), bold_border)
	worksheet.write('P29', '=(region_data!$L${!s}:$L${!s})'.format(T, T), bold_border)
	worksheet.write('P30', '=(region_data!$M${!s}:$M${!s})'.format(T, T), bold_border)
	worksheet.write('P31', '=(region_data!$N${!s}:$N${!s})'.format(T, T), bold_border)
	worksheet.write('P32', '=(region_data!$O${!s}:$O${!s})'.format(T, T), bold_border)
	worksheet.write('P33', '=(region_data!$P${!s}:$P${!s})'.format(T, T), bold_border)
	worksheet.write('P34', '=(region_data!$Q${!s}:$Q${!s})'.format(T, T), bold_border)
	worksheet.write('P35', '=(region_data!$R${!s}:$R${!s})'.format(T, T), bold_border)
	worksheet.write('P36', '=(region_data!$S${!s}:$S${!s})'.format(T, T), bold_border)
	#
	worksheet.write('U27', 'Workstations which failed in one or more categories', bold)
	worksheet.write('T28', 'Cats.', bold_border)
	worksheet.write('U28', 'Workstations', bold_border)
	worksheet.write('T29', 'One', bold_border)
	worksheet.write('T30', 'Two', bold_border)
	worksheet.write('T31', 'Three', bold_border)
	worksheet.write('T32', 'Four', bold_border)
	worksheet.write('U29', '=(region_data!$G${!s}:$G${!s})'.format(T, T), bold_border)
	worksheet.write('U30', '=(region_data!$H${!s}:$H${!s})'.format(T, T), bold_border)
	worksheet.write('U31', '=(region_data!$I${!s}:$I${!s})'.format(T, T), bold_border)
	worksheet.write('U32', '=(region_data!$J${!s}:$J${!s})'.format(T, T), bold_border)


	#
	# wrap it up
	workbook.close()


#

def send_xlsx():

	print "\n\nSending Email....\n\n"
	# create string from BASE_DN_LIST
	# BASE_DN_LIST_STR = ''.join(BASE_DN_LIST)
	
	# create list of ous
	ou_list = '\n\n'.join('{} : {}'.format(key, val) for key, val in sorted(REGION_DICT.items()))
	
	# get current date
	d = datetime.datetime.now()
	# send to everyone on a Monday
	if d.isoweekday() == 1:
		SMTP_ADDR = SMTP_ADDR_ALL
	# send to just to me and Matt Gurney every other day
	else:
		SMTP_ADDR = SMTP_ADDR_MGURNEY
		
	msg = MIMEMultipart()
	msg['From'] = SMTP_FROM
	msg['To'] = ", ".join(SMTP_ADDR)
	msg['Date'] = formatdate(localtime = True)
	msg['Subject'] = 'Workstation State Spreadsheet'
	body = "Hello,\n\nPlease see attached spreadsheet. \n\n\
	****** \n\n \
	Script is held in \\\\zed-mokeefe-linux3\\data\\projects\\ws_state \n\n \
	This spreadsheet has data on the current state of workstations from the following AD containers: \n\n " \
	+ ou_list + "\n\n \
	It aggregates machine data from multiple workstation OUs. It assigns scores based on presence in Carbon Black, McAfee and SCCM, \n \
	the Last Update times from McAfee, Carbon Black and SCCM (Hardware and Software), SCCM Compliance status and Bitlocker presence in AD for laptops.\n\n \
	NOTE:\n\n \
	bl status = 1 			: object is laptop and does not have bitlocker key in ad \n \
	cb status = 1 			: object does not exist in Carbon Black \n \
	mcafee status =1 		: object does not exist in McAfee \n \
	sc status =1 			: object does not exist in SCCM \n \
	cb date check = 1		: no cb update in last 30 days \n \
	mcafee date check = 1		: no mcafee update in last 30 days \n \
	sc hw date check = 1		: no sc hardware update in last 30 days \n \
	sc sw date check = 1		: no sc software update in last 30 days \n \
	sccm compliance = 1		: machine is not sccm compliant \n\n \
	The number of patches which have been deployed but are missing is also included for each machine, as well as tallies of missing patches by OU and region (1 to 10, 11 to 20 etc). \n \n \
	The total cb, mc, sccm and bl scores are then aggregated across region and OU for individual machine failures. \n \
	Separate aggregate scores are also created for machines which have had at least one failure in multiple categories. \n\n \
	The machine status in ad can be determined as follows: \n \
	ad status = 4096		: ad object is enabled \n \
	ad status = 4098		: ad object is disabled \n\n \
	For explanation of other ad status codes see: http://jackstromberg.com/2013/01/useraccountcontrol-attributeflag-values/ \n\n \
	*** This is an automated message. Do not reply to this address. ***" \
	
	content = MIMEText(body, 'plain')
	msg.attach(content)
	
	part = MIMEBase('application', "octet-stream")
	part.set_payload(open("computer_state.xlsx", "rb").read())
	encoders.encode_base64(part)
	part.add_header('Content-Disposition', 'attachment; filename="computer_state.xlsx"')
	msg.attach(part)
	
	smtp = smtplib.SMTP(SMTP_HOST, 25)
	smtp.sendmail(SMTP_FROM, SMTP_ADDR, msg.as_string())
	smtp.quit()


def connect_es():
	es = elasticsearch.Elasticsearch(['https://' + ES_USER + ':' + ES_PASS + '@' + ES_HOST + ':9200'], ca_certs='/etc/elasticsearch/certs/domainRootCA.cer', \
	client_cert='/etc/elasticsearch/certs/elk5.crt', client_key='/etc/elasticsearch/certs/elk5.key' )
	index = 'cg-computers-' + datetime.datetime.now().strftime('%y-%m-%d-%H')

def create_mapping():
	mapping = {
		"mappings": {
			"_default_": {
				"properties": {
					"query_time": {
						"type": "date",
						"format": "yyyy-MM-dd HH:mm:ss" ,
						"time_zone": "-5:00",
						"null_value": "NULL",
						"index": "not_analyzed",
						"doc_values": True
					},
					"host_name": {
						"type": "text",
						"null_value": "NULL",
						"index": "not_analyzed",
						"doc_values": True
					},
					"ad_status": {
						"type": "integer",
						"null_value": "NULL",
						"index": "not_analyzed",
						"doc_values": True
					},
					"cb_status": {
						"type": "integer",
						"null_value": "NULL",
						"index": "not_analyzed",
						"doc_values": True
					},
					"cb_date_check": {
						"type": "integer",
						"null_value": "NULL",
						"index": "not_analyzed",
						"doc_values": True
					},
					"mc_status": {
						"type": "integer",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"mc_date_check": {
						"type": "integer",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"sc_status": {
						"type": "integer",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"sc_hw_score": {
						"type": "integer",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"sc_sw_score": {
						"type": "integer",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"sc_comp_score": {
						"type": "integer",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"ou": {
						"type": "text",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"region": {
						"type": "text",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"cb_id": {
						"type": "text",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"cb_group": {
						"type": "text",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"cb_os": {
						"type": "text",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"cb_datetime": {
						"type": "date",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"mc_datetime": {
						"type": "date",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"user": {
						"type": "text",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"site": {
						"type": "text",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"sccm_os": {
						"type": "text",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"sccm_sp": {
						"type": "text",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"compliance": {
						"type": "text",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"sc_hw_datetime": {
						"type": "date",
						"format": "yyyy-MM-dd HH:mm:ss" ,
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"sc_sw_datetime": {
						"type": "date",
						"format": "yyyy-MM-dd HH:mm:ss" ,
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"hardware": {
						"type": "text",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"bl_status": {
						"type": "integer",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"ad_description": {
						"type": "text",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					},
					"sc_serial_number": {
						"type": "text",
						"null_value": "NULL" ,
						"index": "not_analyzed",
						"doc_values": True
					}
				}
			}
		}
	}


    # write the index
def write_index():
	es.indices.create(index=index, ignore=400, body=mapping)

	# get current time
	now = datetime.datetime.now()

	# for each computer, create an index entry 
	for computer in computer_dict_all:
		body = {
			'query_time':         now,
			'host_name':          computer_dict_all[computer]["cn"],
			'ad_status':          computer_dict_all[computer]["ad_status"],
			'cb_status':          computer_dict_all[computer]["cb_status"],
			'cb_date_check':      computer_dict_all[computer]["cb_date_check"],
			'mc_status':          computer_dict_all[computer]["mc_status"],
			'mc_date_check':      computer_dict_all[computer]["mc_date_check"],
			'sc_status':          computer_dict_all[computer]["sc_status"],
			'sc_hw_date_check':   computer_dict_all[computer]["sc_hw_date_check"],
			'sc_sw_date_check':   computer_dict_all[computer]["sc_sw_date_check"],
			'sc_comp_check':      computer_dict_all[computer]["sc_comp_check"],
			'ou':                 computer_dict_all[computer]["ou"],
			'region':             computer_dict_all[computer]["region"],
			'cb_id':              computer_dict_all[computer]["cb_id"],
			'cb_group':           computer_dict_all[computer]["cb_group"],
			'cb_os':              computer_dict_all[computer]["cb_os"],
			'cb_datetime':        computer_dict_all[computer]["cb_datetime"],
			'mc_datetime':        computer_dict_all[computer]["mc_datetime"],
			'user':               computer_dict_all[computer]["sc_user"],
			'site':               computer_dict_all[computer]["sc_site"],
			'sc_os':              computer_dict_all[computer]["sc_os"],
			'sc_sp':              computer_dict_all[computer]["sc_sp"],
			'compliance':         computer_dict_all[computer]["sc_compliance"],
			'sc_hw_datetime':     computer_dict_all[computer]["sc_hw_datetime"],
			'sc_sw_datetime':     computer_dict_all[computer]["sc_sw_datetime"],
			'hardware':           computer_dict_all[computer]["hardware"],
			'bl_status':          computer_dict_all[computer]["bl_status"],
			'ad_description':     computer_dict_all[computer]["desc"],
			'sc_serial_number':   computer_dict_all[computer]["sc_serial_number"]
			}
		es.index(index, 'cg-computer', body)
        

#
	
if __name__ == "__main__":
	# execute only if run as a script

	# create flat list of all ous and regions
	# create counter for regions and ous
	# used to determine row count and plot chart data
	#
	# create score dictionary for each region and ou
	#
	# generate computer list from each ou
	# 
	print "\n\nQuerying Active Directory....\n\n"
	for region, ou_list in REGION_DICT.iteritems():
		region_all.append(region)
		region_count += 1
		
		# initialize scores dictionary for region
		region_scores_dict[region] = { \
		"cb_score"         : 0, \
		"mc_score"         : 0, \
		"sc_score"         : 0, \
		"bl_score"         : 0, \
		"0_cat"            : 0, \
		"1_cat"            : 0, \
		"2_cat"            : 0, \
		"3_cat"            : 0, \
		"4_cat"            : 0, \
		"fully_patched"    : 0, \
		"1_10_missing"     : 0, \
		"11_20_missing"    : 0, \
		"21_30_missing"    : 0, \
		"31_40_missing"    : 0, \
		"41_50_missing"    : 0, \
		"51_100_missing"   : 0, \
		"over_100_missing" : 0, \
		"not_fully_patched": 0, \
		"unknown_patches"  : 0, \
		"region"           : region \
		}
		
		for ou in ou_list:
			ou_all.append(ou)
			ou_count += 1
			
			# initialize scores dictionary for ou
			ou_scores_dict[ou] = { \
			"cb_score"         : 0, \
			"mc_score"         : 0, \
			"sc_score"         : 0, \
			"bl_score"         : 0, \
			"0_cat"            : 0, \
			"1_cat"            : 0, \
			"2_cat"            : 0, \
			"3_cat"            : 0, \
			"4_cat"            : 0, \
			"fully_patched"    : 0, \
			"1_10_missing"     : 0, \
			"11_20_missing"    : 0, \
			"21_30_missing"    : 0, \
			"31_40_missing"    : 0, \
			"41_50_missing"    : 0, \
			"51_100_missing"   : 0, \
			"over_100_missing" : 0, \
			"not_fully_patched": 0, \
			"unknown_patches"  : 0, \
			"region"           : region, \
			"ou"             : ou \
			}
			# query ad for computer list
			get_computers_ad(ou, region)
			
	# generate and add sensor list from cb
	get_computers_cb()
	
	# print(computer_dict_all)
	
	# generate computer list from mcafee
	get_computers_mc()

	#	print(computer_dict_all)
	
	# generate computer list from sccm
	get_computers_sccm()

	#	print(computer_dict_all)
			
	# produce ou and region aggregate scores
	ou_region_aggregate(computer_dict_all)
	
#	print(region_scores_dict)
	
	# print("\n\n\n")
	
#	print(ou_scores_dict)

	# remove the old spreadsheet if it exists
	try:
		os.remove("/opt/computer_state/computer_state.xlsx")
	except OSError:
		pass
	
	# use json to test output
	#with open('output.txt', 'w') as f:
	#	json.dump(computer_dict_all, f)
	
	# create xlsx of output
	write_xlsx(computer_dict_all)
	
	# send xlsx file as attachment
	send_xlsx()
	
	# break ldap connection
	con.unbind_s()
	
	print "\n\nBye!\n\n"
	
	
# load data into elasticsearch
# define elasticsearch connection and index fields
#
#es = elasticsearch.Elasticsearch(hosts=[ ES_HOST + ':9200'], http_auth=(ES_USER, ES_PASS))
#es = elasticsearch.Elasticsearch(['https://' + ES_USER + ':' + ES_PASS + '@' + ES_HOST + ':9200'] )
