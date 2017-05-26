#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author:       Matthew O'Keefe
# Date:         2017 May
#
# Description:  Get list of users from specific containers in ad
#               Target security OUs to find exception groups using
#               name patterns and attempt to categorise them based on 
#               group name. 
#               If non user member found, treat it as a group 
#               and query membership recursively.
#               Query Elasticsearch to find all additions 
#               removals for last 31 days.
#               Aggregate scores by region and ou.
#               Output to Excel.
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
SMTP_HOST = 'mail.domain.com'
SMTP_FROM = 'm_okeefe@domain.com'
SMTP_ADDR = ['m_okeefe@domain.com', ]


# define elasticsearch indexer
ES_HOST = 'zed-elastic5.domain.com'
# define elasticsearch credentials
ES_USER = 'admin'
ES_PASS = 'xxxxxx'


# define ldap parameters
AD_SERVER = 'ldap://zed-dc1.domain.com:389'

# define credentials
AD_USER = 'CN=svc_elastic_ldap,OU=Vulcan,OU=Coolio,OU=Service Accounts,DC=domain,DC=com'
AD_PASS = 'xxxxxx@'

# tell script to query AD and find groups, so we can assign them to categories
GROUP_SEARCH_TARGETS = [ 'CN=Coolio,OU=Security,OU=Groups,DC=domain,DC=com', \
'CN=Barsoom,OU=Security,OU=Groups,DC=domain,DC=com', \
'CN=US,OU=Security,OU=Groups,DC=domain,DC=com', \
'CN=Europe,OU=Security,OU=Groups,DC=domain,DC=com']

REGION_DICT     = { \
'WESTERN Coolio' : ('OU=Users,OU=Western Coolio,OU=Coolio,OU=domain,DC=domain,DC=com', \
'OU=Shared Accounts,OU=Western Coolio,OU=Coolio,OU=domain,DC=domain,DC=com'), \
'EASTERN Coolio' : ('OU=Users,OU=Eastern Coolio,OU=Coolio,OU=domain,DC=domain,DC=com', \
'OU=Shared Accounts,OU=Eastern Coolio,OU=Coolio,OU=domain,DC=domain,DC=com'), \
'Coolio'         : ('CN=Consultants,OU=Coolio,OU=domain,DC=domain,DC=com', \
'OU=Systems Admins,OU=Coolio,OU=domain,DC=domain,DC=com'), \
'ASIA'           : ('OU=Users,OU=Asia,OU=Asia Pacific,OU=domain,DC=domain,DC=com', \
'OU=Shared Accounts,OU=Asia,OU=Asia Pacific,OU=domain,DC=domain,DC=com'), \
'Arse'      : ('OU=Users,OU=Arse,OU=Asia Pacific,OU=domain,DC=domain,DC=com',), \
'ASIA PACIFIC'   : ('OU=Systems Admins,OU=Asia Pacific,OU=domain,DC=domain,DC=com',), \
'Barsoom' : ('OU=Users,OU=VENUS,OU=Barsoom,OU=domain,DC=domain,DC=com', \
'OU=Shared Accounts,OU=VENUS,OU=Barsoom,OU=domain,DC=domain,DC=com', \
'OU=Shared Accounts,OU=Operations Centre,OU=Oley,OU=Barsoom,OU=domain,DC=domain,DC=com', \
'OU=Users,OU=IT,OU=Operations Centre,OU=Oley,OU=Barsoom,OU=domain,DC=domain,DC=com', \
'OU=Users,OU=Operations Centre,OU=Oley,OU=Barsoom,OU=domain,DC=domain,DC=com', \
'OU=Users,OU=Moldavia,OU=Oley,OU=Barsoom,OU=domain,DC=domain,DC=com', \
'OU=Shared Accounts,OU=Moldavia,OU=Oley,OU=Barsoom,OU=domain,DC=domain,DC=com', \
'OU=Test Users,OU=Oley,OU=Barsoom,OU=domain,DC=domain,DC=com', \
'OU=Users,OU=IT,OU=Moldavia,OU=Oley,OU=Barsoom,OU=domain,DC=domain,DC=com', \
'OU=Users,OU=Isle of Man,OU=Barsoom,OU=domain,DC=domain,DC=com', \
'OU=Shared Accounts,OU=Isle of Man,OU=Barsoom,OU=domain,DC=domain,DC=com', \
'OU=Systems Admins,OU=Barsoom,OU=domain,DC=domain,DC=com', \
'OU=Greebo,OU=Barsoom,OU=domain,DC=domain,DC=com'), \
'EUROPE'         : ('OU=Greebo,OU=Europe,OU=domain,DC=domain,DC=com', \
'CN=Consultants,OU=Europe,OU=domain,DC=domain,DC=com', \
'OU=Users,OU=Europe,OU=domain,DC=domain,DC=com', \
'OU=Shared Accounts,OU=Europe,OU=domain,DC=domain,DC=com'), \
'UAE'            : ('OU=Users,OU=Dubai,OU=UAE,OU=domain,DC=domain,DC=com', \
'OU=Systems Admins,OU=UAE,OU=domain,DC=domain,DC=com', \
'CN=Consultants,OU=UAE,OU=domain,DC=domain,DC=com'), \
'USA'         : ('OU=Users,OU=USA,OU=domain,DC=domain,DC=com', \
'OU=Shared Accounts,OU=USA,OU=domain,DC=domain,DC=com', \
'OU=Systems Admins,OU=USA,OU=domain,DC=domain,DC=com', \
'CN=Consultants,OU=USA,OU=domain,DC=domain,DC=com'), \
'RETIRED': ('OU=Asia Pacific,OU=Users,OU=Retired,DC=domain,DC=com', \
'OU=Coolio,OU=Users,OU=Retired,DC=domain,DC=com', \
'OU=Barsoom,OU=Users,OU=Retired,DC=domain,DC=com', \
'OU=Europe,OU=Users,OU=Retired,DC=domain,DC=com', \
'OU=USA,OU=Users,OU=Retired,DC=domain,DC=com',) \
}

	
REGION_DICT_WC = {'WESTERN Coolio' : ('OU=Users,OU=Western Coolio,OU=Coolio,OU=domain,DC=domain,DC=com',) }

REGION_DICT_SHORT = {'WESTERN Coolio' : ('OU=Users,OU=Western Coolio,OU=Coolio,OU=domain,DC=domain,DC=com', \
'OU=Shared Accounts,OU=Western Coolio,OU=Coolio,OU=domain,DC=domain,DC=com'), \
'EASTERN Coolio' : ('OU=Users,OU=Eastern Coolio,OU=Coolio,OU=domain,DC=domain,DC=com', \
'OU=Shared Accounts,OU=Eastern Coolio,OU=Coolio,OU=domain,DC=domain,DC=com'), \
'Coolio' : ('CN=Consultants,OU=Coolio,OU=domain,DC=domain,DC=com', \
'OU=Systems Admins,OU=Coolio,OU=domain,DC=domain,DC=com'), \
'USA'         : ('OU=Users,OU=USA,OU=domain,DC=domain,DC=com', \
'OU=Shared Accounts,OU=USA,OU=domain,DC=domain,DC=com', \
'OU=Systems Admins,OU=USA,OU=domain,DC=domain,DC=com', \
'CN=Consultants,OU=USA,OU=domain,DC=domain,DC=com')}

# Category list

CAT_LIST = ("Botnets", "File Sharing", "Gambling", "Games", "Hacking", \
"Hate", "High Risk", "Illegal Drugs", "Pornography", "Spam", "Tasteless", \
"Torrents", "Webmail", "SSL", "Social", "Remote", "Software", "Streaming", "Unknown") 


def convert_ldaptime(ldaptime):
	unixtime = ((int(ldaptime)/10000000)-11644473600)
	return (datetime.datetime.fromtimestamp(int(unixtime)).strftime('%Y-%m-%d %H:%M:%S'))

class AdConnection:
	def __init__(self, AD_SERVER, AD_USER, AD_PASS):
		self.con = ldap.initialize(AD_SERVER)
		self.con.simple_bind_s(AD_USER, AD_PASS)

class UserExceptions:
	def __init__(self, con, REGION_DICT):
		global region_all
		global region_count
		global ou_all
		global ou_count
		

		for region, ou_list in REGION_DICT.iteritems():
		
			region_all.append(region)
			region_count += 1
			
			# initialize scores dictionary for region
			region_scores_dict[region] = { \
			"Botnets"       : 0, \
			"File Sharing"  : 0, \
			"Gambling"      : 0, \
			"Games"         : 0, \
			"Hacking"       : 0, \
			"Hate"          : 0, \
			"High Risk"     : 0, \
			"Illegal Drugs" : 0, \
			"Pornography"   : 0, \
			"Spam"          : 0, \
			"Tasteless"     : 0, \
			"Torrents"      : 0, \
			"Webmail"       : 0, \
			"SSL"           : 0, \
			"Social"        : 0, \
			"Remote"        : 0, \
			"Software"      : 0, \
			"ag_score"      : 0, \
			"Unknown"       : 0, \
			"Streaming"     : 0, \
			"region"        : region \
			}
			
			# initialize addition counts for region
			region_add_dict[region] = { \
			"Botnets"       : 0, \
			"File Sharing"  : 0, \
			"Gambling"      : 0, \
			"Games"         : 0, \
			"Hacking"       : 0, \
			"Hate"          : 0, \
			"High Risk"     : 0, \
			"Illegal Drugs" : 0, \
			"Pornography"   : 0, \
			"Spam"          : 0, \
			"Tasteless"     : 0, \
			"Torrents"      : 0, \
			"Webmail"       : 0, \
			"SSL"           : 0, \
			"Social"        : 0, \
			"Remote"        : 0, \
			"Software"      : 0, \
			"Unknown"       : 0, \
			"ag_score"      : 0, \
			"Streaming"     : 0, \
			"region"        : region \
			}
			
			for ou in ou_list:
				ou_all.append(ou)
				ou_count += 1
				
				# initialize scores dictionary for ou
				ou_scores_dict[ou] = { \
				"Botnets"       : 0, \
				"File Sharing"  : 0, \
				"Gambling"      : 0, \
				"Games"         : 0, \
				"Hacking"       : 0, \
				"Hate"          : 0, \
				"High Risk"     : 0, \
				"Illegal Drugs" : 0, \
				"Pornography"   : 0, \
				"Spam"          : 0, \
				"Tasteless"     : 0, \
				"Torrents"      : 0, \
				"Webmail"       : 0, \
				"SSL"           : 0, \
				"Social"        : 0, \
				"Remote"        : 0, \
				"Software"      : 0, \
				"ag_score"      : 0, \
				"Unknown"       : 0, \
				"Streaming"     : 0, \
				"region"        : region, \
				"ou"            : ou \
				}
	
				# initialize addition counts dictionary for ou
				ou_add_dict[ou] = { \
				"Botnets"       : 0, \
				"File Sharing"  : 0, \
				"Gambling"      : 0, \
				"Games"         : 0, \
				"Hacking"       : 0, \
				"Hate"          : 0, \
				"High Risk"     : 0, \
				"Illegal Drugs" : 0, \
				"Pornography"   : 0, \
				"Spam"          : 0, \
				"Tasteless"     : 0, \
				"Torrents"      : 0, \
				"Webmail"       : 0, \
				"SSL"           : 0, \
				"Social"        : 0, \
				"Remote"        : 0, \
				"Software"      : 0, \
				"Unknown"       : 0, \
				"ag_score"      : 0, \
				"Streaming"     : 0, \
				"region"        : region, \
				"ou"            : ou \
				}
			
				# call class to query AD
				ad_users = AdUsers(ou, region, con)

		# Get group information
		Ad_Groups = AdGroups(con, GROUP_SEARCH_TARGETS)
		
		# Get aggregate scores
		ou_region_aggregates = OuRegionAggregates(user_dict_exceptions)

class AdUsers:
	def __init__(self, ou, region, con):
		global user_dict_master
		print "Getting AD users....\n\n"
		print ou
		# default query attributes
		FILTER = '(&(objectClass=user))'
		ATTRS = ['cn', 'description', 'mailNickname', 'badPwdCount', 'badPasswordTime', \
		'pwdLastSet', 'sAMAccountName', 'userPrincipalName', 'lastLogonTimestamp', 'distinguishedName' ]
		
		#print "Searching OU = ", ou
		#search OU for computers
		results = con.search_s(ou, ldap.SCOPE_SUBTREE, FILTER , ATTRS) 
		#print "\n\n"
		# extract user data from AD results
		for CN in results:
			user_object            = CN[1]['distinguishedName'][0]
			user_cn                = CN[1]['cn'][0]
			try:
				pwdLastSet         = convert_ldaptime(CN[1]['pwdLastSet'][0])
			except:
				pwdLastSet         = CN[1]['pwdLastSet'][0]
			try:
				sAMAccountName     = CN[1]['sAMAccountName'][0]
			except:
				sAMAccountName     = 'None'
			try:
				userPrincipalName  = CN[1]['userPrincipalName'][0]
			except:
				userPrincipalName  = 'None'
			try:
				lastLogonTimestamp = convert_ldaptime(CN[1]['lastLogonTimestamp'][0])
			except:
				lastLogonTimestamp = 'Never'
			try:
				badPwdCount     = int(CN[1]['badPwdCount'][0])
			except:
				badPwdCount     = 0
			try:
				badPasswordTime = convert_ldaptime(CN[1]['badPasswordTime'][0])
			except:
				badPasswordTime = 'Never'
			try:
				mailNickname    = CN[1]['mailNickname'][0]
			except:
				mailNickname    = "None"
			try: 
				ad_description = CN[1]['description'][0]
			except:
				ad_description = "None"
			#
			try:
				unicode(user_cn, "ascii")
			except UnicodeError:
				user_cn = unicode(user_cn, "utf-8")
			else:
				pass
			try:
				unicode(sAMAccountName, "ascii")
			except UnicodeError:
				sAMAccountName = unicode(sAMAccountName, "utf-8")
			else:
				pass
			try:
				unicode(userPrincipalName, "ascii")
			except UnicodeError:
				userPrincipalName = unicode(userPrincipalName, "utf-8")
			else:
				pass
			try:
				unicode(ad_description, "ascii")
			except UnicodeError:
				ad_description = unicode(ad_description, "utf-8")
			else:
				pass
			try:
				unicode(user_object, "ascii")
			except UnicodeError:
				user_object = unicode(user_object, "utf-8")
			else:
				pass
			# print user_cn
			surname    = user_cn.split(",", 1)[0]
			#print surname
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
			"desc"              : ad_description, \
			"mailNickname"      : mailNickname, \
			"badPwdCount"       : badPwdCount, \
			"badPasswordTime"   : badPasswordTime, \
			"pwdLastSet"        : pwdLastSet, \
			"sAMAccountName"    : sAMAccountName, \
			"userPrincipalName" : userPrincipalName, \
			"lastLogonTimestamp": lastLogonTimestamp, \
			"user_object"       : user_object, \
			"Botnets"           : 0, \
			"File Sharing"      : 0, \
			"Gambling"          : 0, \
			"Games"             : 0, \
			"Hacking"           : 0, \
			"Hate"              : 0, \
			"High Risk"         : 0, \
			"Illegal Drugs"     : 0, \
			"Pornography"       : 0, \
			"Spam"              : 0, \
			"Tasteless"         : 0, \
			"Torrents"          : 0, \
			"Webmail"           : 0, \
			"SSL"               : 0, \
			"Social"            : 0, \
			"Remote"            : 0, \
			"Software"          : 0, \
			"Streaming"         : 0, \
			"ag_score"          : 0, \
			"Unknown"           : 0 \
			}
			# add to master dictionary
			user_dict_master[user_object] = user_dict_ad

class AdGroups:
	def __init__(self, con, GROUP_SEARCH_TARGETS):
		print "Getting AD Groups...."
		# default query attributes
		FILTER_GR = '(&(objectClass=group))'
		ATTRS_GR = ['cn', 'description']
		global group_cat_dict
		for ou in GROUP_SEARCH_TARGETS:
			results = con.search_s(ou, ldap.SCOPE_SUBTREE, FILTER_GR , ATTRS_GR) 
		
			for CN in results:
				group_object        = CN[0]
				group_cn                = CN[1]['cn'][0]
				# filter exception groups and categorise them
				if ("Allow Browsing" in group_cn) or ("URL Allow" in group_cn):
					if "Botnets" in group_cn:
						group_cat_dict[group_cn] = "Botnets"
					elif ("File Sharing" in group_cn) or ("Storage Sharing" in group_cn) \
					or ("Google Docs" in group_cn) or ("P2P" in group_cn) or ("Google Drive" in group_cn) or ("FTP" in group_cn):
						group_cat_dict[group_cn]  = "File Sharing"
					elif ("Gambling" in group_cn):
						group_cat_dict[group_cn]  = "Gambling"
					elif ("Games" in group_cn):
						group_cat_dict[group_cn]  = "Games"
					elif ("Hacking" in group_cn) or ("Malicious" in group_cn):
						group_cat_dict[group_cn]  = "Hacking"
					elif ("Hate" in group_cn):
						group_cat_dict[group_cn]  = "Hate"
					elif ("High Risk" in group_cn) or ("Medium Risk" in group_cn):
						group_cat_dict[group_cn]  = "High Risk"
					elif ("Illegal Drugs" in group_cn):
						group_cat_dict[group_cn]  = "Illegal Drugs"
					elif ("Pornography" in group_cn) or ("Adult Material" in group_cn):
						group_cat_dict[group_cn]  = "Pornography"
					elif ("Spam" in group_cn):
						group_cat_dict[group_cn]  = "Spam"
					elif ("Tasteless" in group_cn):
						group_cat_dict[group_cn]  = "Tasteless"
					elif ("Torrents" in group_cn):
						group_cat_dict[group_cn]  = "Torrents"
					elif ("Webmail" in group_cn) or ("Allow Email" in group_cn) or ("Instant Messaging" in group_cn):
						group_cat_dict[group_cn]  = "Webmail"
					elif ("SSL" in group_cn):
						group_cat_dict[group_cn]  = "SSL"
					elif ("Social Networking" in group_cn) or ("Social Media" in group_cn) or ("Twitter" in group_cn):
						group_cat_dict[group_cn]  = "Social"
					elif ("Allow Remote" in group_cn):
						group_cat_dict[group_cn]  = "Remote"
					elif ("Software Download" in group_cn):
						group_cat_dict[group_cn]  = "Software"
					elif ("Streaming" in group_cn):
						group_cat_dict[group_cn]  = "Streaming"
					else:
						group_cat_dict[group_cn]  = "Unknown"
					print group_cn + " = " + group_cat_dict[group_cn]
					print "\n"
					category = group_cat_dict[group_cn]
					
					group_members = GroupMembers(group_cn, group_object, category)

class GroupMembers:
	def __init__(self, group_cn, group_object, category):
		print "Getting Group Members"
		global user_dict_master
		global user_dict_exceptions
		global group_members_dict
		global ou_scores_dict
		global nested_groups
		
		group_members_list = []
		
		ou_start   = ",CN="
		try:
			group_ou = "OU=" + (group_object).split(ou_start, 1)[1]
		except:
			group_ou = "unknown"
		
		region_end = ",OU="
		try:
			group_region = ((group_object).split(ou_start, 1)[1]).split(region_end, 1)[0]
		except:
			group_region = "unknown"
		
#		print user_dict_master
		results = con.search_s(group_object, ldap.SCOPE_BASE)
		
		for result in results:
			result_dn = result[0]
			result_attrs = result[1]
#
			# if a member is found, update the dictionary for that member and group
			if "member" in result_attrs:
				# member is a distinguishedName
				for member in result_attrs["member"]:
					member_details = {}
					try:
						user_dict_master[member][category] += 1
						# add to exceptions dictionary
						user_dict_exceptions[member] = user_dict_master[member]
					
					except:
						print "Group member not found in AD! ", group_cn, "member = ", member
						# try recursively to check for members in case it is a group
						try:
							grp_start = "CN="
							grp_end = ","
							cn_end = "OU="
							cn_slash = ((member).split(grp_start, 1)[1]).split(grp_end, 1)[0]
							try:
								cn = (cn_slash).split("\\", 1)[1]
							except:
								cn = cn_slash
							nested_test = GroupMembers(cn, member, category)
							nested_groups.extend([cn])
							
							try:
								ou = "OU=" + (member).split(cn_end, 1)[1]
							except:
								ou = ""
						except:
							pass
							
						# create exceptions dict entry
						try: 
							user_dict_exceptions[member][category] += 1
						except:
							# add the nested group to the exceptions dictionary
							user_dict_exceptions[member] = { \
								"surname"           : cn, \
								"given_name"        : "***Nested Group***", \
								"ou"                : ou , \
								"region"            : "***Nested Group***", \
								"desc"              : "***Nested Group***", \
								"mailNickname"      : "", \
								"badPwdCount"       : "", \
								"badPasswordTime"   : "", \
								"pwdLastSet"        : "", \
								"sAMAccountName"    : "", \
								"userPrincipalName" : "", \
								"lastLogonTimestamp": "", \
								"user_object"       : member, \
								"Botnets"           : 0, \
								"File Sharing"      : 0, \
								"Gambling"          : 0, \
								"Games"             : 0, \
								"Hacking"           : 0, \
								"Hate"              : 0, \
								"High Risk"         : 0, \
								"Illegal Drugs"     : 0, \
								"Pornography"       : 0, \
								"Spam"              : 0, \
								"Tasteless"         : 0, \
								"Torrents"          : 0, \
								"Webmail"           : 0, \
								"SSL"               : 0, \
								"Social"            : 0, \
								"Remote"            : 0, \
								"Software"          : 0, \
								"Streaming"         : 0, \
								"ag_score"          : 0, \
								"Unknown"           : 0 \
								}
							# add count to category for nested group
							user_dict_exceptions[member][category] += 1
							print user_dict_exceptions[member]
							
					group_members_list.extend([member])
		
		# add list to global dictionary of exception group members
		group_members_dict[group_cn] = { \
		"object"        : group_object, \
		"category"      : category, \
		"group_ou"      : group_ou, \
		"group_region"  : group_region, \
		"members"       : group_members_list \
		}

class OuRegionAggregates:
	def __init__(self, user_dict_exceptions):
		print "\n\nCalculating Aggregate Scores....\n\n"
		global ou_scores_dict
		global region_scores_dict
		for user in user_dict_exceptions:
			ou     = user_dict_exceptions[user]["ou"]
			region = user_dict_exceptions[user]["region"]
			# ag_score counts number of exceptions for this user
			user_dict_exceptions[user]["ag_score"] = 0
			for category in CAT_LIST:
				if user_dict_exceptions[user][category] >= 1:
					user_dict_exceptions[user]["ag_score"] += 1
					try:
						region_scores_dict[region][category] += 1
						ou_scores_dict[ou][category]         += 1
						# aggregate scores
						region_scores_dict[region]["ag_score"] += 1
						ou_scores_dict[ou]["ag_score"]         += 1
					except:
						pass

class EsQuery:
	def __init__(self, ES_USER, ES_PASS, ES_HOST):
		# define connection to elasticsearch
		es = elasticsearch.Elasticsearch(['https://' + ES_USER + ':' + ES_PASS + '@' + ES_HOST + ':9200'], verify_certs=False)
		
		# define time and query each index in date range
		for x in range (0,31):
			date = str(datetime.date.today() + datetime.timedelta(-x))
			dotdate = date[0:4] + '.' + date[5:7] + '.' + date[8:]
			print "date = " , dotdate
			es_index = 'logstash-winlogbeat-' + dotdate
			print "index = " , es_index
			
			event_id_list = ["4728", "4729"]
			for event_id in event_id_list:
				try:
					es_response = es.search(index=es_index, body={"query": \
					{"bool" : { "must" : [ { "match": { "event_id" : event_id }}], \
					"should": [ {"match_phrase": { "group_name" : "Allow Browsing" }} , \
					{"match_phrase": { "group_name" : "SG CI URL Allow" }}], "minimum_should_match": 1}}, \
					"fields": ["record_number","action", "group_name", "account_name", "@timestamp", "subject_account"]})
				except:
					print "es query error"
				# write results to dictionary
				results = es_response["hits"]["hits"]
				es_analysis = EsAnalysis(results)

class EsExtras:
	def __init__(self, nested_groups, ES_USER, ES_PASS, ES_HOST):
		# define connection to elasticsearch
		es = elasticsearch.Elasticsearch(['https://' + ES_USER + ':' + ES_PASS + '@' + ES_HOST + ':9200'], verify_certs=False)
		
		# define time and query each index in date range
		for x in range (0,31):
			date = str(datetime.date.today() + datetime.timedelta(-x))
			dotdate = date[0:4] + '.' + date[5:7] + '.' + date[8:]
			print "date = " , dotdate
			es_index = 'logstash-winlogbeat-' + dotdate
			print "index = " , es_index
			
			event_id_list = ["4728", "4729"]
			for event_id in event_id_list:
				for group in nested_groups:
					try:
						es_response = es.search(index=es_index, body={"query": \
						{"bool" : { "must" : [ { "match": { "event_id" : event_id }}], \
						"must": [ {"match_phrase": { "group_name" : group }} ]}}, \
						"fields": ["record_number","action", "group_name", "account_name", "@timestamp", "subject_account"]})
					except:
						print "es query error"
					# write results to dictionary
					results = es_response["hits"]["hits"]
					es_analysis = EsAnalysis(results)

class EsAnalysis:
	def __init__(self, results):
		global group_actions_dict
		global ou_scores_dict
		global ou_add_dict
		global region_add_dict
	
		
		for hit in results:
			group_actions_record = {}

			#print "\n\n"
			#print hit["fields"]
			#print "\n\n"
			# 
			rec_num = str(hit["fields"]["record_number"][0])
			
			start_u = "CN="
			start_l = "cn="
			div   = "\, "
			end   = ",OU="
			account_string = str(hit["fields"]["account_name"][0])
			
			if div in account_string:
				try: 
					surname = (str(hit["fields"]["account_name"][0])).split(start_u)[1].split(div)[0]
				except KeyError:
					surname = (str(hit["fields"]["account_name"][0])).split(start_l)[1].split(div)[0]
				except:
					pass
			else:
				try:
					surname = (str(hit["fields"]["account_name"][0])).split(start_u)[1].split(end, 1)[0]
				except KeyError:
					surname = (str(hit["fields"]["account_name"][0])).split(start_l)[1].split(end, 1)[0]
				except:
					pass
			try:
				given_name = (str(hit["fields"]["account_name"][0])).split(div)[1].split(end)[0]
			except:
				given_name = ""

			try: 
				action_ou  = "OU=" + (str(hit["fields"]["account_name"][0])).split(end, 1)[1]
			except:
				action_ou  = "unknown"

			try:
				action_region = ou_scores_dict[action_ou]["region"] 
			except:
				action_region = "unknown"

			try:
				subject_account = str(hit["fields"]["subject_account"][0])
			except:
				subject_account = "unknown"
			
				# extract given and surname and build dictionary of events
			group_actions_record = { \
			"account_name" : str(hit["fields"]["account_name"][0]), \
			"group_name"   : str(hit["fields"]["group_name"][0]), \
			"date"         : (str(hit["fields"]["@timestamp"][0])).split("T")[0], \
			"time"         : (str(hit["fields"]["@timestamp"][0])).split("T")[1], \
			"action"       : str(hit["fields"]["action"][0]), \
			"surname"      : surname, \
			"given_name"   : given_name, \
			"ou"           : action_ou, \
			"region"       : action_region, \
			"admin"        : subject_account, \
			"category"     : "Unknown" \
			}
			
			# match category
			for group, category in group_cat_dict.iteritems():
				if group_actions_record["group_name"] == group:
					group_actions_record["category"] = category

		
			if group_actions_record["action"] == "Added to group":
			# increment add event counts
				try:
					ou_add_dict[group_actions_record["ou"]][group_actions_record["category"]] += 1
					ou_add_dict[group_actions_record["ou"]]["ag_score"] += 1
				except:
					pass
				try:
					region_add_dict[group_actions_record["region"]][group_actions_record["category"]] += 1
					region_add_dict[group_actions_record["region"]]["ag_score"] += 1
				except:
					pass
			
			
			# add to global dictionary
			group_actions_dict[rec_num] = group_actions_record

class WriteXL:
	def __init__(self, user_dict_exceptions):
		print "Creating Spreadsheet Page 1....\n\n"
	
		# remove the old spreadsheet if it exists
		try:
			os.remove("/data/projects/user_state/user_exceptions.xlsx")
		except OSError:
			pass
		
		# change directory
		os.chdir ('/data/projects/user_state')
		
		# create a spreadsheet of results	
		workbook = xlsxwriter.Workbook('user_exceptions.xlsx')
		
		#####################################################
		# create first page
		# page 1 summary data
		
		worksheet = workbook.add_worksheet('current members')
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
		worksheet.write('A1',  'Surname', bold_rotate)
		worksheet.write('B1',  'Given Name', bold_rotate)
		worksheet.write('C1',  'Exception Count', bold_rotate)
		worksheet.write('D1',  'Description', bold_rotate)
		worksheet.write('E1',  'Botnets', bold_rotate)
		worksheet.write('F1',  'File Sharing', bold_rotate)
		worksheet.write('G1',  'Gambling', bold_rotate)
		worksheet.write('H1',  'Games', bold_rotate)
		worksheet.write('I1',  'Hacking', bold_rotate)
		worksheet.write('J1',  'Hate', bold_rotate)
		worksheet.write('K1',  'High Risk', bold_rotate)
		worksheet.write('L1',  'Illegal Drugs', bold_rotate)
		worksheet.write('M1',  'Pornography', bold_rotate)
		worksheet.write('N1',  'Spam', bold_rotate)
		worksheet.write('O1',  'Tasteless', bold_rotate)
		worksheet.write('P1',  'Torrents', bold_rotate)
		worksheet.write('Q1',  'Webmail', bold_rotate)
		worksheet.write('R1',  'SSL', bold_rotate)
		worksheet.write('S1',  'Social', bold_rotate)
		worksheet.write('T1',  'Remote', bold_rotate)
		worksheet.write('U1',  'Software', bold_rotate)
		worksheet.write('V1',  'Streaming', bold_rotate)
		worksheet.write('W1',  'Unknown', bold_rotate)
		worksheet.write('X1',  'OU', bold_rotate)
		worksheet.write('Y1',  'Region', bold_rotate)
		worksheet.write('Z1',  'mailNickname', bold_rotate)
		worksheet.write('AA1',  'Bad Password Count', bold_rotate)
		worksheet.write('AB1',  'Bad Password Time', bold_rotate)
		worksheet.write('AC1',  'Password Last Set', bold_rotate)
		worksheet.write('AD1',  'SAM Account Name', bold_rotate)
		worksheet.write('AE1',  'User Principal Name', bold_rotate)
		worksheet.write('AF1',  'Last Logon', bold_rotate)
		worksheet.write('AG1',  'User Object', bold_rotate)

	
		# Start from the first cell below the headers.
		row = 1
		col = 0
	
		for user in user_dict_exceptions:
	
			worksheet.write (row, col,      user_dict_exceptions[user]["surname"])
			worksheet.write (row, col + 1,   user_dict_exceptions[user]["given_name"])
			worksheet.write (row, col + 2,  user_dict_exceptions[user]["ag_score"], bold)
			worksheet.write (row, col + 3,  user_dict_exceptions[user]["desc"])
			worksheet.write (row, col + 4, user_dict_exceptions[user]["Botnets"])
			worksheet.write (row, col + 5, user_dict_exceptions[user]["File Sharing"])
			worksheet.write (row, col + 6, user_dict_exceptions[user]["Gambling"])
			worksheet.write (row, col + 7, user_dict_exceptions[user]["Games"])
			worksheet.write (row, col + 8, user_dict_exceptions[user]["Hacking"])
			worksheet.write (row, col + 9, user_dict_exceptions[user]["Hate"])
			worksheet.write (row, col + 10, user_dict_exceptions[user]["High Risk"])
			worksheet.write (row, col + 11, user_dict_exceptions[user]["Illegal Drugs"])
			worksheet.write (row, col + 12, user_dict_exceptions[user]["Pornography"])
			worksheet.write (row, col + 13, user_dict_exceptions[user]["Spam"])
			worksheet.write (row, col + 14, user_dict_exceptions[user]["Tasteless"])
			worksheet.write (row, col + 15, user_dict_exceptions[user]["Torrents"])
			worksheet.write (row, col + 16, user_dict_exceptions[user]["Webmail"])
			worksheet.write (row, col + 17, user_dict_exceptions[user]["SSL"])
			worksheet.write (row, col + 18, user_dict_exceptions[user]["Social"])
			worksheet.write (row, col + 19, user_dict_exceptions[user]["Remote"])
			worksheet.write (row, col + 20, user_dict_exceptions[user]["Software"])
			worksheet.write (row, col + 21, user_dict_exceptions[user]["Streaming"])
			worksheet.write (row, col + 22, user_dict_exceptions[user]["Unknown"])
			worksheet.write (row, col + 23,  user_dict_exceptions[user]["ou"])
			worksheet.write (row, col + 24,  user_dict_exceptions[user]["region"])
			worksheet.write (row, col + 25,  user_dict_exceptions[user]["mailNickname"])
			worksheet.write (row, col + 26,  user_dict_exceptions[user]["badPwdCount"])
			worksheet.write (row, col + 27,  user_dict_exceptions[user]["badPasswordTime"])
			worksheet.write (row, col + 28,  user_dict_exceptions[user]["pwdLastSet"])
			worksheet.write (row, col + 29,  user_dict_exceptions[user]["sAMAccountName"])
			worksheet.write (row, col + 30,  user_dict_exceptions[user]["userPrincipalName"])
			worksheet.write (row, col + 31, user_dict_exceptions[user]["lastLogonTimestamp"])
			worksheet.write (row, col + 32, user_dict_exceptions[user]["user_object"])


			# write next row
			row += 1
	
		# insert filters on columns
		worksheet.autofilter(0, 0, row, 32)
	
		# calculate sums on scores
		Formula1  = '=SUM(C1:C{!s})'.format(row)
		Formula2  = '=SUM(E1:E{!s})'.format(row)
		Formula3  = '=SUM(F1:F{!s})'.format(row)
		Formula4  = '=SUM(G1:G{!s})'.format(row)
		Formula5  = '=SUM(H1:H{!s})'.format(row)
		Formula6  = '=SUM(I1:I{!s})'.format(row)
		Formula7  = '=SUM(J1:J{!s})'.format(row)
		Formula8  = '=SUM(K1:K{!s})'.format(row)
		Formula9  = '=SUM(L1:L{!s})'.format(row)
		Formula10  = '=SUM(M1:M{!s})'.format(row)
		Formula11 = '=SUM(N1:N{!s})'.format(row)
		Formula12 = '=SUM(O1:O{!s})'.format(row)
		Formula13 = '=SUM(P1:P{!s})'.format(row)
		Formula14 = '=SUM(Q1:Q{!s})'.format(row)
		Formula15 = '=SUM(R1:R{!s})'.format(row)
		Formula16 = '=SUM(S1:S{!s})'.format(row)
		Formula17 = '=SUM(T1:T{!s})'.format(row)
		Formula18 = '=SUM(U1:U{!s})'.format(row)
		Formula19 = '=SUM(V1:V{!s})'.format(row)
		Formula20 = '=SUM(W1:W{!s})'.format(row)
	
		# Add totals
		worksheet.write (row, 0, 'TOTALS', bold)
		worksheet.write (row, 2,  Formula1, bold)
		worksheet.write (row, 4, Formula2, bold)
		worksheet.write (row, 5, Formula3, bold)
		worksheet.write (row, 6, Formula4, bold)
		worksheet.write (row, 7, Formula5, bold)
		worksheet.write (row, 8, Formula6, bold)
		worksheet.write (row, 9, Formula7, bold)
		worksheet.write (row, 10, Formula8, bold)
		worksheet.write (row, 11, Formula9, bold)
		worksheet.write (row, 12, Formula10, bold)
		worksheet.write (row, 13, Formula11, bold)
		worksheet.write (row, 14, Formula12, bold)
		worksheet.write (row, 15, Formula13, bold)
		worksheet.write (row, 16, Formula14, bold)
		worksheet.write (row, 17, Formula15, bold)
		worksheet.write (row, 18, Formula16, bold)
		worksheet.write (row, 19, Formula17, bold)
		worksheet.write (row, 20, Formula18, bold)
		worksheet.write (row, 21, Formula19, bold)
		worksheet.write (row, 22, Formula20, bold)
		#

		########################################################################################
		# page 2
		# create second page for ou counts
		#
		print "Creating Spreadsheet Page 2....\n\n"
		worksheet = workbook.add_worksheet('count by ou')
		worksheet.freeze_panes(1, 0)
		
		# write headers
		worksheet.write('A1', 'OU', bold_rotate)
		worksheet.write('B1', 'Region', bold_rotate)
		worksheet.write('C1',  'Botnets', bold_rotate)
		worksheet.write('D1',  'File Sharing', bold_rotate)
		worksheet.write('E1',  'Gambling', bold_rotate)
		worksheet.write('F1',  'Games', bold_rotate)
		worksheet.write('G1',  'Hacking', bold_rotate)
		worksheet.write('H1',  'Hate', bold_rotate)
		worksheet.write('I1',  'High Risk', bold_rotate)
		worksheet.write('J1',  'Illegal Drugs', bold_rotate)
		worksheet.write('K1',  'Pornography', bold_rotate)
		worksheet.write('L1',  'Spam', bold_rotate)
		worksheet.write('M1',  'Tasteless', bold_rotate)
		worksheet.write('N1',  'Torrents', bold_rotate)
		worksheet.write('O1',  'Webmail', bold_rotate)
		worksheet.write('P1',  'SSL', bold_rotate)
		worksheet.write('Q1',  'Social', bold_rotate)
		worksheet.write('R1',  'Remote', bold_rotate)
		worksheet.write('S1',  'Software', bold_rotate)
		worksheet.write('T1',  'Streaming', bold_rotate)
		worksheet.write('U1',  'Unknown', bold_rotate)
		worksheet.write('V1',  'TOTAL', bold_rotate)
		
		# Start from the first cell below the headers.
		row = 1
		col = 0
	
		# write aggregate scores for each ou
		# for ou, ou_stats in ou_scores_dict.iteritems():
		
		for ou in ou_scores_dict:
		
			worksheet.write (row, col,       ou_scores_dict[ou]["ou"])
			worksheet.write (row, col + 1,   ou_scores_dict[ou]["region"])
			worksheet.write (row, col + 2,   ou_scores_dict[ou]["Botnets"])
			worksheet.write (row, col + 3,   ou_scores_dict[ou]["File Sharing"])
			worksheet.write (row, col + 4,   ou_scores_dict[ou]["Gambling"])
			worksheet.write (row, col + 5,   ou_scores_dict[ou]["Games"])
			worksheet.write (row, col + 6,   ou_scores_dict[ou]["Hacking"])
			worksheet.write (row, col + 7,   ou_scores_dict[ou]["Hate"])
			worksheet.write (row, col + 8,   ou_scores_dict[ou]["High Risk"])
			worksheet.write (row, col + 9,   ou_scores_dict[ou]["Illegal Drugs"])
			worksheet.write (row, col + 10,  ou_scores_dict[ou]["Pornography"])
			worksheet.write (row, col + 11,  ou_scores_dict[ou]["Spam"])
			worksheet.write (row, col + 12,  ou_scores_dict[ou]["Tasteless"])
			worksheet.write (row, col + 13,  ou_scores_dict[ou]["Torrents"])
			worksheet.write (row, col + 14,  ou_scores_dict[ou]["Webmail"])
			worksheet.write (row, col + 15,  ou_scores_dict[ou]["SSL"])
			worksheet.write (row, col + 16,  ou_scores_dict[ou]["Social"])
			worksheet.write (row, col + 17,  ou_scores_dict[ou]["Remote"])
			worksheet.write (row, col + 18,  ou_scores_dict[ou]["Software"])
			worksheet.write (row, col + 19,  ou_scores_dict[ou]["Streaming"])
			worksheet.write (row, col + 20,  ou_scores_dict[ou]["Unknown"])
			worksheet.write (row, col + 21,  ou_scores_dict[ou]["ag_score"], bold)
			row += 1
			col  = 0
		
		# insert filters on columns
		worksheet.autofilter(0, 0, row, 21)
		
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
		Formula19 = '=SUM(U1:U{!s})'.format(row)
	
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
		worksheet.write (row, 20, Formula19, bold)

		########################################################################################
		# page 3
		# create third page for region counts
		#
		print "Creating Spreadsheet Page 3....\n\n"
		worksheet = workbook.add_worksheet('count by region')
		worksheet.freeze_panes(1, 0)
		
		# write headers
		# write headers
		worksheet.write('A1', 'Region', bold_rotate)
		worksheet.write('B1',  'Botnets', bold_rotate)
		worksheet.write('C1',  'File Sharing', bold_rotate)
		worksheet.write('D1',  'Gambling', bold_rotate)
		worksheet.write('E1',  'Games', bold_rotate)
		worksheet.write('F1',  'Hacking', bold_rotate)
		worksheet.write('G1',  'Hate', bold_rotate)
		worksheet.write('H1',  'High Risk', bold_rotate)
		worksheet.write('I1',  'Illegal Drugs', bold_rotate)
		worksheet.write('J1',  'Pornography', bold_rotate)
		worksheet.write('K1',  'Spam', bold_rotate)
		worksheet.write('L1',  'Tasteless', bold_rotate)
		worksheet.write('M1',  'Torrents', bold_rotate)
		worksheet.write('N1',  'Webmail', bold_rotate)
		worksheet.write('O1',  'SSL', bold_rotate)
		worksheet.write('P1',  'Social', bold_rotate)
		worksheet.write('Q1',  'Remote', bold_rotate)
		worksheet.write('R1',  'Software', bold_rotate)
		worksheet.write('S1',  'Streaming', bold_rotate)
		worksheet.write('T1',  'Unknown', bold_rotate)
		worksheet.write('U1',  'TOTAL', bold_rotate)
		
		# Start from the first cell below the headers.
		row = 1
		col = 0
		
		# write aggregate scores for each region
		#for region, region_stats in region_scores_dict.iteritems():
		
		for region in region_scores_dict:
		
			worksheet.write (row, col,      region_scores_dict[region]["region"])
			worksheet.write (row, col + 1,  region_scores_dict[region]["Botnets"])
			worksheet.write (row, col + 2,  region_scores_dict[region]["File Sharing"])
			worksheet.write (row, col + 3,  region_scores_dict[region]["Gambling"])
			worksheet.write (row, col + 4,  region_scores_dict[region]["Games"])
			worksheet.write (row, col + 5,  region_scores_dict[region]["Hacking"])
			worksheet.write (row, col + 6,  region_scores_dict[region]["Hate"])
			worksheet.write (row, col + 7,  region_scores_dict[region]["High Risk"])
			worksheet.write (row, col + 8,  region_scores_dict[region]["Illegal Drugs"])
			worksheet.write (row, col + 9,  region_scores_dict[region]["Pornography"])
			worksheet.write (row, col + 10, region_scores_dict[region]["Spam"])
			worksheet.write (row, col + 11,  region_scores_dict[region]["Tasteless"])
			worksheet.write (row, col + 12,  region_scores_dict[region]["Torrents"])
			worksheet.write (row, col + 13,  region_scores_dict[region]["Webmail"])
			worksheet.write (row, col + 14,  region_scores_dict[region]["SSL"])
			worksheet.write (row, col + 15,  region_scores_dict[region]["Social"])
			worksheet.write (row, col + 16,  region_scores_dict[region]["Remote"])
			worksheet.write (row, col + 17,  region_scores_dict[region]["Software"])
			worksheet.write (row, col + 18,  region_scores_dict[region]["Streaming"])
			worksheet.write (row, col + 19,  region_scores_dict[region]["Unknown"])
			worksheet.write (row, col + 20,  region_scores_dict[region]["ag_score"] , bold)
			row += 1
			col  = 0

		# insert filters on columns
		worksheet.autofilter(0, 0, row, 20)
		
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
		Formula19 = '=SUM(T1:T{!s})'.format(row)
		
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
		worksheet.write (row, 19, Formula19, bold)

		#####################################################
		# create fourth page
		# page 4 group events
		print "Creating Spreadsheet Page 4....\n\n"
		worksheet = workbook.add_worksheet('group changes last 31 days')
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
		worksheet.write('A1',  'Date', bold_rotate)
		worksheet.write('B1',  'Time', bold_rotate)
		worksheet.write('C1',  'Given Name', bold_rotate)
		worksheet.write('D1',  'Surname', bold_rotate)
		worksheet.write('E1',  'Action', bold_rotate)
		worksheet.write('F1',  'Category', bold_rotate)
		worksheet.write('G1',  'Group Name', bold_rotate)
		worksheet.write('H1',  'Admin', bold_rotate)
		worksheet.write('I1',  'User Region', bold_rotate)
		worksheet.write('J1',  'User OU', bold_rotate)
		
		 # Start from the first cell below the headers.
		row = 1
		col = 0
		
		for event in group_actions_dict:
		
			worksheet.write (row, col,      group_actions_dict[event]["date"]   )
			worksheet.write (row, col + 1,  group_actions_dict[event]["time"]   )
			worksheet.write (row, col + 2,  group_actions_dict[event]["given_name"]   )
			worksheet.write (row, col + 3,  group_actions_dict[event]["surname"]   )
			worksheet.write (row, col + 4,  group_actions_dict[event]["action"]   )
			worksheet.write (row, col + 5,  group_actions_dict[event]["category"]   )
			worksheet.write (row, col + 6,  group_actions_dict[event]["group_name"]   )
			worksheet.write (row, col + 7,  group_actions_dict[event]["admin"]   )
			worksheet.write (row, col + 8,  group_actions_dict[event]["region"]   )
			worksheet.write (row, col + 9,  group_actions_dict[event]["ou"]   )

			# write next row
			row += 1
		
		# insert filters on columns
		worksheet.autofilter(0, 0, row, 9)
	##
		########################################################################################
		# page 5
		# create fifth page for ou events
		#
		print "Creating Spreadsheet Page 5....\n\n"
		worksheet = workbook.add_worksheet('added by ou last 31 days')
		worksheet.freeze_panes(1, 0)
		
		# write headers
		worksheet.write('A1', 'OU', bold_rotate)
		worksheet.write('B1', 'Region', bold_rotate)
		worksheet.write('C1',  'Botnets', bold_rotate)
		worksheet.write('D1',  'File Sharing', bold_rotate)
		worksheet.write('E1',  'Gambling', bold_rotate)
		worksheet.write('F1',  'Games', bold_rotate)
		worksheet.write('G1',  'Hacking', bold_rotate)
		worksheet.write('H1',  'Hate', bold_rotate)
		worksheet.write('I1',  'High Risk', bold_rotate)
		worksheet.write('J1',  'Illegal Drugs', bold_rotate)
		worksheet.write('K1',  'Pornography', bold_rotate)
		worksheet.write('L1',  'Spam', bold_rotate)
		worksheet.write('M1',  'Tasteless', bold_rotate)
		worksheet.write('N1',  'Torrents', bold_rotate)
		worksheet.write('O1',  'Webmail', bold_rotate)
		worksheet.write('P1',  'SSL', bold_rotate)
		worksheet.write('Q1',  'Social', bold_rotate)
		worksheet.write('R1',  'Remote', bold_rotate)
		worksheet.write('S1',  'Software', bold_rotate)
		worksheet.write('T1',  'Streaming', bold_rotate)
		worksheet.write('U1',  'Unknown', bold_rotate)
		worksheet.write('V1',  'TOTAL', bold_rotate)
		
		# Start from the first cell below the headers.
		row = 1
		col = 0
		
		# write aggregate scores for each ou
		# for ou, ou_stats in ou_scores_dict.iteritems():
		
		for ou in ou_add_dict:
		
			worksheet.write (row, col,       ou_add_dict[ou]["ou"])
			worksheet.write (row, col + 1,   ou_add_dict[ou]["region"])
			worksheet.write (row, col + 2,   ou_add_dict[ou]["Botnets"])
			worksheet.write (row, col + 3,   ou_add_dict[ou]["File Sharing"])
			worksheet.write (row, col + 4,   ou_add_dict[ou]["Gambling"])
			worksheet.write (row, col + 5,   ou_add_dict[ou]["Games"])
			worksheet.write (row, col + 6,   ou_add_dict[ou]["Hacking"])
			worksheet.write (row, col + 7,   ou_add_dict[ou]["Hate"])
			worksheet.write (row, col + 8,   ou_add_dict[ou]["High Risk"])
			worksheet.write (row, col + 9,   ou_add_dict[ou]["Illegal Drugs"])
			worksheet.write (row, col + 10,  ou_add_dict[ou]["Pornography"])
			worksheet.write (row, col + 11,  ou_add_dict[ou]["Spam"])
			worksheet.write (row, col + 12,  ou_add_dict[ou]["Tasteless"])
			worksheet.write (row, col + 13,  ou_add_dict[ou]["Torrents"])
			worksheet.write (row, col + 14,  ou_add_dict[ou]["Webmail"])
			worksheet.write (row, col + 15,  ou_add_dict[ou]["SSL"])
			worksheet.write (row, col + 16,  ou_add_dict[ou]["Social"])
			worksheet.write (row, col + 17,  ou_add_dict[ou]["Remote"])
			worksheet.write (row, col + 18,  ou_add_dict[ou]["Software"])
			worksheet.write (row, col + 19,  ou_add_dict[ou]["Streaming"])
			worksheet.write (row, col + 20,  ou_add_dict[ou]["Unknown"])
			worksheet.write (row, col + 21,  ou_add_dict[ou]["ag_score"] , bold)
			row += 1
			col  = 0
		
		# insert filters on columns
		worksheet.autofilter(0, 0, row, 21)
		
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
		Formula19 = '=SUM(U1:U{!s})'.format(row)
		
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
		worksheet.write (row, 20, Formula19, bold)
		
		#
		########################################################################################
		# page 6
		# create sixth page for region events
		#
		print "Creating Spreadsheet Page 6....\n\n"
		worksheet = workbook.add_worksheet('added by regions last 31 days')
		worksheet.freeze_panes(1, 0)
		
		# write headers
		# write headers
		worksheet.write('A1', 'Region', bold_rotate)
		worksheet.write('B1',  'Botnets', bold_rotate)
		worksheet.write('C1',  'File Sharing', bold_rotate)
		worksheet.write('D1',  'Gambling', bold_rotate)
		worksheet.write('E1',  'Games', bold_rotate)
		worksheet.write('F1',  'Hacking', bold_rotate)
		worksheet.write('G1',  'Hate', bold_rotate)
		worksheet.write('H1',  'High Risk', bold_rotate)
		worksheet.write('I1',  'Illegal Drugs', bold_rotate)
		worksheet.write('J1',  'Pornography', bold_rotate)
		worksheet.write('K1',  'Spam', bold_rotate)
		worksheet.write('L1',  'Tasteless', bold_rotate)
		worksheet.write('M1',  'Torrents', bold_rotate)
		worksheet.write('N1',  'Webmail', bold_rotate)
		worksheet.write('O1',  'SSL', bold_rotate)
		worksheet.write('P1',  'Social', bold_rotate)
		worksheet.write('Q1',  'Remote', bold_rotate)
		worksheet.write('R1',  'Software', bold_rotate)
		worksheet.write('S1',  'Streaming', bold_rotate)
		worksheet.write('T1',  'Unknown', bold_rotate)
		worksheet.write('U1',  'TOTAL', bold_rotate)
		
		# Start from the first cell below the headers.
		row = 1
		col = 0
		
		# write aggregate scores for each region
		#for region, region_stats in region_scores_dict.iteritems():
		
		for region in region_add_dict:
		
			worksheet.write (row, col,      region_add_dict[region]["region"])
			worksheet.write (row, col + 1,  region_add_dict[region]["Botnets"])
			worksheet.write (row, col + 2,  region_add_dict[region]["File Sharing"])
			worksheet.write (row, col + 3,  region_add_dict[region]["Gambling"])
			worksheet.write (row, col + 4,  region_add_dict[region]["Games"])
			worksheet.write (row, col + 5,  region_add_dict[region]["Hacking"])
			worksheet.write (row, col + 6,  region_add_dict[region]["Hate"])
			worksheet.write (row, col + 7,  region_add_dict[region]["High Risk"])
			worksheet.write (row, col + 8,  region_add_dict[region]["Illegal Drugs"])
			worksheet.write (row, col + 9,  region_add_dict[region]["Pornography"])
			worksheet.write (row, col + 10, region_add_dict[region]["Spam"])
			worksheet.write (row, col + 11,  region_add_dict[region]["Tasteless"])
			worksheet.write (row, col + 12,  region_add_dict[region]["Torrents"])
			worksheet.write (row, col + 13,  region_add_dict[region]["Webmail"])
			worksheet.write (row, col + 14,  region_add_dict[region]["SSL"])
			worksheet.write (row, col + 15,  region_add_dict[region]["Social"])
			worksheet.write (row, col + 16,  region_add_dict[region]["Remote"])
			worksheet.write (row, col + 17,  region_add_dict[region]["Software"])
			worksheet.write (row, col + 18,  region_add_dict[region]["Streaming"])
			worksheet.write (row, col + 19,  region_add_dict[region]["Unknown"])
			worksheet.write (row, col + 20,  region_add_dict[region]["ag_score"]  , bold   )
			row += 1
			col  = 0

		# insert filters on columns
		worksheet.autofilter(0, 0, row, 20)
		
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
		Formula19 = '=SUM(T1:T{!s})'.format(row)
		
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
		worksheet.write (row, 19, Formula19, bold)

	#
		########################################################################################
		# page 7
		# create seventh page for full list 
		#
		print "Creating Spreadsheet Page 7....\n\n"
		worksheet = workbook.add_worksheet('full list exception members')
		worksheet.freeze_panes(1, 0)
		
		# write headers
		# write headers
		worksheet.write('A1', 'Surname', bold_rotate)
		worksheet.write('B1', 'Given Name', bold_rotate)
		worksheet.write('C1', 'Group', bold_rotate)
		worksheet.write('D1', 'Category', bold_rotate)
		worksheet.write('E1', 'User OU', bold_rotate)
		worksheet.write('F1', 'User Region', bold_rotate)
		worksheet.write('G1', 'Group OU', bold_rotate)
		worksheet.write('H1', 'Group Region', bold_rotate)
		
		# Start from the first cell below the headers.
		row = 1
		col = 0
		
		# get properties for each group
		for group, group_properties in group_members_dict.iteritems():
			# write a row for each member
			for member in group_properties["members"]:
				#
				if member in user_dict_exceptions:
					try:
						member_surname = user_dict_exceptions[member]["surname"]
					except:
						member_surname = "unknown"
					#
					try:
						member_given_name = user_dict_exceptions[member]["given_name"]
					except:
						member_given_name = "unknown"
					#
					try:
						member_ou = user_dict_exceptions[member]["ou"]
					except:
						member_ou = "unknown"
					#
					try:
						member_region = user_dict_exceptions[member]["region"] 
					except:
						member_region = "unknown"
					#
					worksheet.write (row, col,  member_surname )
					worksheet.write (row, col + 1, member_given_name )
					worksheet.write (row, col + 2, group )
					worksheet.write (row, col + 3, group_properties["category"] )
					worksheet.write (row, col + 4, member_ou )
					worksheet.write (row, col + 5, member_region )
					worksheet.write (row, col + 6, group_properties["group_ou"] )
					worksheet.write (row, col + 7, group_properties["group_region"] )
					
					row += 1
					col  = 0

		# insert filters on columns
		worksheet.autofilter(0, 0, row, 7)
		


	#

		##############################################################################
		#
		# define row counts for ou and region charts
		
		O = ou_count 
		R = region_count

		
		# wrap it up
		workbook.close()

class SendPackage:
	def __init__(self):
		print "Sending report as email attachment....\n"

		msg = MIMEMultipart()
		msg['From'] = SMTP_FROM
		msg['To'] = ", ".join(SMTP_ADDR)
		msg['Date'] = formatdate(localtime = True)
		msg['Subject'] = 'User Exception Group Membership'
		body = "Hello,\n\nPlease see attached spreadsheet, showing \
a breakdown of memberships of user exception groups by ou and region. \
Because we have multiple similar groups globally, the summary page lists members by \
category of group rather than per individual group, but a full \
list of exception groups and members  is provided on the last worksheet. \n\n\
Page 1: summary of current members of exceptions groups by category \n\n\
Page 2: member count of exceptions groups by ou \n\n\
Page 3: member count of exceptions groups by region \n\n\
Page 4: additions to and removals from groups in the last 31 days \n\n\
Page 5: count of additions to groups by ou in the last 31 days \n\n\
Page 6: count of additions to groups by region in the last 31 days \n\n\
Page 7: full list of all current members of exception groups \n\n\
We look for exception groups in the following OUs: \n\n"\
+ str(GROUP_SEARCH_TARGETS) + "\n\n\
In those OUs, we look for Group names which contain the following patterns: \n\n\
\"Allow Browsing\" or \"URL Allow\"\n\n\
We try to assign them a category based on their name. If we can't, they are added to the \"Unknown\" category. \n\n\
If we find an object in a group which is not an AD user, we assume \
it is itself a group and check recursively for members. \
We add it as a group with the same category as its parent, and its membership is then also listed on Page 7.\n\n\
************\n\n"
		content = MIMEText(body, 'plain')
		msg.attach(content)
		
		part = MIMEBase('application', "octet-stream")
		part.set_payload(open("/data/projects/user_state/user_exceptions.xlsx", "rb").read())
		encoders.encode_base64(part)
		part.add_header('Content-Disposition', 'attachment; filename="user_exceptions.xlsx"')
		msg.attach(part)
		
		smtp = smtplib.SMTP(SMTP_HOST, 25)
		smtp.sendmail(SMTP_FROM, SMTP_ADDR, msg.as_string())
		smtp.quit()
'''
"Never rub another man's rhubarb"
'''

if __name__ == "__main__":
	group_cat_dict = {}
	user_dict_master = {}
	user_dict_exceptions = {}
	group_actions_dict = {}
	group_members_dict = {}

	# initialize counters for ou and regions 
	ou_all = []
	ou_count = 0
	ou_scores_dict = {}
	ou_add_dict ={}
	region = []
	region_all = []
	region_count = 0
	region_scores_dict = {}
	region_add_dict = {}
	nested_groups = []

	con = ldap.initialize(AD_SERVER)
	con.simple_bind_s(AD_USER, AD_PASS)
	

	# call main class to find exceptions
	User_Exceptions = UserExceptions(con, REGION_DICT)
	
	# print "\n\n"
	# print group_cat_dict
	# print "\n\n"
	
	# print group_members_dict
	
	es_query = EsQuery(ES_USER, ES_PASS, ES_HOST)
	
	es_extras = EsExtras(nested_groups, ES_USER, ES_PASS, ES_HOST)
	
	# print user_dict_master
	#print user_dict_exceptions
	write_xl = WriteXL(user_dict_exceptions)
	send_package = SendPackage()
	
	print nested_groups
	print "\nBye!\n"
#
#
