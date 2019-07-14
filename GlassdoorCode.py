
#import all the libraries
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
from datetime import datetime
import gc
import numpy as np
import re
import datetime as dt
import time
import urllib.request
from bs4 import BeautifulSoup
import http.client
import requests
import json

import random
#Function to extract information from left of the string
def left(a,b):
    return a[:b]
#Function to extract information from right of the string
def right(a,b):
    return a[len(a)-b:]
#Function to remove dupicates from a list
def remove_duplicates(a):
    t = []
    for i in a:
        if i not in t:
            t.append(i)
    return t 

# Variable Declarations 

BaseIndustrylist     = []
BaseIndustryHref     = []
BaseCompanyUrlist    = []
CompanyJobsUrllinks  = [] 
BaseIndustyListFinal = []
BaseSubCategoryList  = []

#Individual Job title extraction section 

process_start_time = dt.datetime.now()

BaseUrl     = 'https://www.glassdoor.com/sitedirectory/title-jobs.htm'
BaseReq     = urllib.request.Request(BaseUrl, headers = {'User-Agent': 'Mozilla/5.0'})
Basehtml    = urllib.request.urlopen(BaseReq)
BaseUrlsoup = BeautifulSoup(Basehtml,'html.parser')

#Get all the dataset from firmcontainer

IndustryList = BaseUrlsoup.find_all('div', class_ = "row")
       
for i in IndustryList:
    try:
        if i.find('h2').text.strip() == 'Job Categories': 
            BaseIndustrylist = i
        else:
            '' 
    except:
        continue

for j in BaseIndustrylist.find_all('a'):
     BaseIndustryHref.append(j.get('href'))


for Href in BaseIndustryHref:
  #print(Href)
  BaseJobsUrl  = "https://www.glassdoor.com"+Href
  #print(BaseJobsUrl)
  BaseJobsReq = urllib.request.Request(BaseJobsUrl, headers = {'User-Agent': 'Mozilla/5.0'})
  BaseJobshtml = urllib.request.urlopen(BaseJobsReq).read()
  BaseJobSoup  =  BeautifulSoup(BaseJobshtml, 'html.parser')
  if (BaseJobSoup.find('div',class_ = 'pageNavBar tbl fill')).text == '':
    BaseJobsUrlhearder = (BaseJobSoup.find('div',class_= "row")).find_all('a')
    #BaseJobSoup.find_all('a')
    for JobsUrl in BaseJobsUrlhearder:
        if "Jobs" in (JobsUrl.text) and "/Job/" in (JobsUrl.get('href')):
           BaseCompanyUrlist.append(JobsUrl.get('href')) 
           BaseSubCategoryList.append(((' ').join(JobsUrl.get('href').split('/')[2].split('-')[0:JobsUrl.get('href').split('/')[2].split('-').index('jobs')])).strip())
           ##BaseSubCategoryList.append(right(left(JobsUrl.get('href'), len(JobsUrl.get('href'))-4),len(left(JobsUrl.get('href'), len(JobsUrl.get('href'))-4))-5).strip())
           BaseIndustyListFinal.append(right(left(Href, len(Href)-4), len(left(Href, len(Href)-4))-29).strip())
        else:
           continue
  else:
    for pages in range(1,10):
        try:
              BaseJobsUrls = left(BaseJobsUrl,len(BaseJobsUrl)-4)+"_P"+str(pages)+".htm"
              #print(BaseJobsUrls)
              BaseJobsReq = urllib.request.Request(BaseJobsUrls, headers = {'User-Agent': 'Mozilla/5.0'})
              BaseJobshtml = urllib.request.urlopen(BaseJobsReq).read()
              BaseJobSoup  =  BeautifulSoup(BaseJobshtml, 'html.parser')
              #BaseJobsUrlhearder = BaseJobSoup.find_all('li')#,class_ = 'header')
              #BaseJobsUrlhearder = BaseJobSoup.find('div', class_ = "row")
              BaseJobsUrlhearder = (BaseJobSoup.find('div',class_= "row")).find_all('a')
              
              for JobsUrl in BaseJobsUrlhearder:
                  if "Jobs" in (JobsUrl.text) and "/Job/" in (JobsUrl.get('href')):
                      BaseCompanyUrlist.append(JobsUrl.get('href')) 
                      BaseSubCategoryList.append(((' ').join(JobsUrl.get('href').split('/')[2].split('-')[0:JobsUrl.get('href').split('/')[2].split('-').index('jobs')])).strip())
                      BaseIndustyListFinal.append(right(left(Href, len(Href)-4), len(left(Href, len(Href)-4))-29).strip())

                  else:
                      #BaseCompanyUrlist.append(JobsUrl.get('href'))
                      continue
        except:
            continue
      
print(len(BaseCompanyUrlist))   


IndustryCat_output = pd.DataFrame(columns=['Industry_Main','Industry_Sub']) 
IndustryCat_output['Industry_Main']      = BaseIndustyListFinal
IndustryCat_output['Industry_Sub']       = BaseSubCategoryList

gc.collect() 

import sqlalchemy
params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};server=gd-hh.database.windows.net;database={Databse];uid=[user ID];pwd=[password]") 
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True) 
engine.connect()
#engine.executemany = False
IndustryCat_output.to_sql(name="[Table name]",con=engine,index = False,  if_exists='replace',
                         dtype={'Industry_Main':  sqlalchemy.types.NVARCHAR(length=500) ,
                                'Industry_Sub':  sqlalchemy.types.NVARCHAR(length=500),
                              }
                        
                        )
#scrape the individual company pages to get all the jobs URL
CompanyJobUrl   = []
Company_Name    = []
Job_Industry    = []
Job_Title       = []
Job_Location    = []
Job_Posted_Date = []
DateScraped     = []


for rlen in range(len(BaseCompanyUrlist)): #979 #range(0,1,1):#
    print("sleeping and processing at", rlen)
    time.sleep(random.randint(1,4)/6)
    for company_pages in range(1,31):
        #This cannot be dynamic
        #print(type(Data_Emp_id))
        #print(len(CompanyJobsUrlHeader))    
        CompanyURl = BaseCompanyUrlist[rlen]
        CompanyJobsUrl = "https://www.glassdoor.com"+left(CompanyURl, len(CompanyURl)-4)+"_IP"+str(company_pages)+".htm?fromAge=7"
        
        #CompanyJobsUrl = "https://www.glassdoor.com/Job/administrative-assistant-jobs-SRCH_KO0,24_IP2.htm?radius=25&fromAge=7"
        #print(CompanyJobsUrl)
        try:
            CompanyJobsReq  = urllib.request.Request(CompanyJobsUrl, headers = {'User-Agent': 'Mozilla/5.0'})
            CompanyJobshtml = urllib.request.urlopen(CompanyJobsReq)
            CompanyJobSoup  = BeautifulSoup(CompanyJobshtml, 'html.parser')
            #text = "attrs={"data-emp-}
            #print(CompanyJobSoup)
           # CompanyJobsUrlHeader.append(CompanyJobSoup.find_all('li', class_ = "jl")) #attrs={"data-emp-id" :"124"})
            for li in CompanyJobSoup.find_all('li',class_= "jl"):
                #Companyname = print(li.find('div', class_ ="jobHeader").text)
                #Job_Industry = left(CompanyURl, len(CompanyURl)-4)
                #job_title = li.find('a').text
                #Joblocation = (li.find('div', class_ = "jobInfoItem empLoc").text
                #li.find('span', class_ = "minor").text
                try:
                    CompanyJobUrl.append("https://www.glassdoor.com"+li.find('a').get('href').strip())
                except:
                    continue
                try:
                    Company_Name.append(li.find('div', class_ ="jobHeader").text.strip())
                except:
                    try:
                        Company_Name.append((li.find('div',class_ = "flexbox empLoc")).find('div').text.strip())
                        print("there was an erroR!")
                    except:                         
                        Company_Name.append('')
                try:
                    #Job_Title.append(li.find('a').text)
                    Job_Title.append(li.find('div', class_ = "titleContainer").text.strip())
                except:
                    try:
                        Job_Title.append(li.find('a', class_ = "jobLink jobInfoItem jobTitle").text.strip())
                    except:
                        Job_Title.append('')
                try:
                    #Job_Location.append(li.find('div', class_ = "jobInfoItem empLoc").text)
                    Job_Location.append(li.find('span', class_ = "subtle loc").text.strip())
                except:
                    Job_Location.append('')
                try:
                    #Job_Industry.append(right(left(CompanyURl, len(CompanyURl)-4),len(left(CompanyURl, len(CompanyURl)-4))-5).strip())
                    Job_Industry.append((' ').join(CompanyURl.split('/')[2].split('-')[0:CompanyURl.split('/')[2].split('-').index('jobs')]))
                except:
                    Job_Industry.append('')
                
                try:
                    Job_Posted_Date.append(li.find('span', class_ = "minor").text.strip()) 
                except: 
                    Job_Posted_Date.append('')
                
                try:
                    DateScraped.append((dt.datetime.date(dt.datetime.now())).strftime('%Y-%m-%d')) 
                except:    
                    DateScraped.append('')   
                print(len(CompanyJobUrl))                    
                #for Bref in CompanyJobsUrlHeader:
                #    try:
                #        URL = "https://www.glassdoor.com"+Bref
                #        IndivJobReq  = urllib.request.Request(URL, headers = {'User-Agent': 'Mozilla/5.0'})
                #        IndivJobhtml = urllib.request.urlopen(IndivJobReq)
                #        IndivJobUrl  = IndivJobhtml.geturl()        
                #       #CompanyJobsUrllinks.insert(0,"https://www.glassdoor.com"+CompanyJobsUrlHeader[outlist][inlist].find('a').get('href'))
                #        CompanyJobsUrllinks.append(IndivJobUrl)
                #        print(len(CompanyJobsUrllinks))
                #    except:
                #        continue 
                        #CompanyJobsUrlHeader = CompanyJobSoup.find_all('li', attrs={"data-emp-id" :Data_Emp_id})
            #print(len(CompanyJobsUrlHeader[0]))
        except:
            continue
        
gc.collect() 

glassdoor_output = pd.DataFrame(columns=['Index_id','CompanyJobUrl','Company_Name','Job_Industry','Job_Title','Job_Location','Job_Description','Job_Posted_Date','DateScraped']) 
# Convert list to dataframe
#for q,w,e,r,t,y,u,i,o,p in zip(Index_id,CompanyJobUrl,Company_Name,Job_Industry,Job_Title,Job_Location,Job_Description,Job_Posted_Date,Job_Post_Valid_Date,DateScraped):
#convert list values into array and send it to dataframe
glassdoor_output['Index_id']       = range(1, len(CompanyJobUrl) + 1 ,1)
glassdoor_output['CompanyJobUrl']  = CompanyJobUrl
glassdoor_output['Company_Name']   = Company_Name
glassdoor_output['Job_Industry']   = Job_Industry
glassdoor_output['Job_Title']      = Job_Title
glassdoor_output['Job_Location']   = Job_Location
glassdoor_output['Job_Description']= ''
glassdoor_output['Job_Posted_Date']= Job_Posted_Date
#glassdoor_output['Job_Post_Valid_Date']= Job_Post_Valid_Date
glassdoor_output['DateScraped']    = DateScraped


    

process_end_time = dt.datetime.now()
print("Process Start Time:= "+process_start_time.strftime("%m/%d/%Y, %H:%M:%S"))
print("Process End Time:=   "+process_end_time.strftime("%m/%d/%Y, %H:%M:%S"))
print("Total process Time:= " +str(process_end_time-process_start_time))



process_start_time = dt.datetime.now()

import sqlalchemy
params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};server=[servername];database=[Database];uid=[UserID];pwd=[Database]") 
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True) 
engine.connect()
#engine.executemany = False
glassdoor_output.to_sql(name="TMP_HIRING_GDOOR_USA",con=engine,index = False,  if_exists='replace',chunksize=100000,
                         dtype={'Index_id': sqlalchemy.types.INTEGER() ,
                                'CompanyJobUrl':  sqlalchemy.types.NVARCHAR(length=1500),
                                'Company_Name': sqlalchemy.types.NVARCHAR(length=500),
                                'Job_Industry': sqlalchemy.types.NVARCHAR(length=500),
                                'Job_Title': sqlalchemy.types.NVARCHAR(length=500),
                                'Job_Location': sqlalchemy.types.NVARCHAR(length=255),
                                'Job_Description':sqlalchemy.types.NVARCHAR(length=10),
                                'Job_Posted_Date': sqlalchemy.NVARCHAR(length=255),
                               # 'Job_Post_Valid_Date': sqlalchemy.DateTime(),
                                'DateScraped': sqlalchemy.DateTime()
                              }
                        
                        )


process_start_time = dt.datetime.now()
process_end_time = dt.datetime.now()
print("Database Process Start Time:= "+process_start_time.strftime("%m/%d/%Y, %H:%M:%S"))
print("Database Process End Time:=   "+process_end_time.strftime("%m/%d/%Y, %H:%M:%S"))
print("Total Database Process Time:= " +str(process_end_time-process_start_time))
#print(output)
#Date =  Date+dt.timedelta(days=1)
##Process End time#

