#Import Relevant packages

import pandas as pd
import numpy as np
import re
import time
import gc
import urllib.request
from bs4 import BeautifulSoup
import datetime as dt
from datetime import timedelta, datetime, date
import requests

#Imitialize Lists to be used
val = []
finalval = []
InsideURL = []
FinaInsideURL = []
insertinto = []
result2 = []
Atags = []
resultcol2 = []
tdtags = []
Finalformat = []
FinalStateUrlIndeed = []
tablecitieshref = []
CompanyFinalLink = []
Company = []
JobTitle = []
Location = []
Jobdescription = []
DateScraped=[]
posteddatelist = []
jobposteddate = []
city = []
Ineedjobkey = []
JobCategory = []
USMainCategory = []
USMainSubCategory = []
USMainSubCategoryurl = []

# Initilaize function to remove dulicates from a list
def remove_duplicates(a):
    t = []
    for i in a:
        if i not in t:
            t.append(i)
    return t
# Capture porocess start information
Indeedstarttime = datetime.now().time()
Indeedstarttime1 = datetime.today().strftime('%A')

#Capture categories from website
BaseURL = 'https://www.indeed.com/browsejobs'
Category = urllib.request.urlopen(BaseURL)
soupIndeedCategory = BeautifulSoup(Category, 'html.parser')
AllCategory = soupIndeedCategory.find('table', id='categories')
FinalAllCategory = AllCategory.findAll('td')
#Generate URL for each category
for rng in range(len(FinalAllCategory)):
    print(rng)
    items = FinalAllCategory[rng]
    try:
        GetSubCategory = urllib.request.urlopen("https://www.indeed.com" + items.find('a')['href'])
    except:
        continue
    HTMLSubCategory = BeautifulSoup(GetSubCategory,'html.parser')
    FinalSubCategory = HTMLSubCategory.find('table',id='titles')
    try:
        FinalSubCategory2 = FinalSubCategory.findAll('a', class_="jobTitle")
    except:
        continue
    for k in range(len(FinalSubCategory2)):
        USMainSubCategory.insert(0, FinalSubCategory2[k].text)
        USMainCategory.insert(0,items.text)
        USMainSubCategoryurl.insert(0, "https://www.indeed.com" + FinalSubCategory2[k].get('href') + "&fromage=7&start=")
        
        

        
#xref dataframe
USCategoryXref = pd.DataFrame( columns = ['USMainCategory', 'USMainSubCategory','DateScraped']) 

USCategoryXref['USMainCategory']=USMainCategory
USCategoryXref['USMainSubCategory']=USMainSubCategory
USCategoryXref['DateScraped']=date.today().strftime('%m/%d/%Y')

#output into database
import sqlalchemy
params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};server=gd-hh.database.windows.net;database=Dell_Scraping;uid=gdolliver;pwd=admin1234!") 
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)  
engine.connect()
USCategoryXref.to_sql(name="TMP_HIRING_INDEED_USA_XREF",
                       con=engine, 
                       index=False, 
                       #chunksize=1000,
                       if_exists='replace')

# Preventing lockout from webste 
Indeedcounterlimit = 2000
Indeedsleepcounter = 0
Pausecounter = 0

Running sourcing process on captured URLs
for u,v in zip(USMainSubCategoryurl,USMainSubCategory):
    Indeedsleepcounter = Indeedsleepcounter + 1
    print(u)

    CategoryUrlhtml = urllib.request.urlopen(u)
    
    CategoryUrlhtmlsoup = BeautifulSoup(CategoryUrlhtml,'html.parser')
    
    CategoryUrlPagesVar = CategoryUrlhtmlsoup.find('div', class_='resultsTop')
    
    try:    
        FinalCategoryUrlPagesVar = CategoryUrlPagesVar.find('div',id='searchCount')
    except:
        continue
        
    try:
        TJobs = FinalCategoryUrlPagesVar.text
        A = re.split(' ',str(TJobs))
        A = A[-2]
    except:        
        A = 1
        #pass
    
    if re.findall(",", str(A)):
        A = re.sub(",", "",A)
        CatFinalPages = (int(A)//12) + 1        
    elif re.findall(".", str(A)):
        A = A.replace(".", "")
        CatFinalPages = (int(A)//12) + 1
    else:
        A
        CatFinalPages = (int(A)//12) + 1
   
                
    if CatFinalPages == 0:
        print("Skipped")
        continue
    elif CatFinalPages > 100:
        for pages in range(0, 1000, 10):
            print("SP GT "+ str(pages))
            
            if Pausecounter <= Indeedcounterlimit:
                time.sleep(0.2)
            else:
                Indeedcounterlimit = Indeedcounterlimit + 100000
                print("sleeping for 5min...")
                #Sleep process for 5 min
                time.sleep(10)

            Mainurl = u + str(pages)
            
            try:
                html = urllib.request.urlopen(Mainurl)
            except:
                continue
            
            try:
                checkresponse1 = html.geturl()
                html  = urllib.request.urlopen(checkresponse1)
            except:
                continue
            
            ResultsColsoup = BeautifulSoup(html,'html.parser')       
            resultslist = ResultsColsoup.find_all(id = 'resultsCol')
            
            #resultslist.find('div', class_='jobsearch-SerpJobCard unifiedRow row result clickcard')
            
            try:
                listofjobsvar = resultslist[0].findAll('div', class_='jobsearch-SerpJobCard')
            except:
                continue
            
            for l in range(len(listofjobsvar)):
                Pausecounter = Pausecounter + 1
                
                
                #Job URL
                try:
                    CompanyFinalLink.insert(0, "https://www.indeed.com" + listofjobsvar[l].find('a')['href'].strip())
                except:
                    CompanyFinalLink.insert(0, '')
                
                #Job Title
                try:
                    JobTitle.insert(0, listofjobsvar[l].find('div', class_='title').text.strip())
                except:
                    JobTitle.insert(0, '')
                #Company name
                try:
                    Company.insert(0,listofjobsvar[l].find('span', class_='company').text.strip())
                except:
                    Company.insert(0, '')
                #location
                
                try:
                    try:
                        Location.insert(0, listofjobsvar[l].find('div', class_='location').text.strip())
                    except:
                        Location.insert(0, listofjobsvar[l].find('span', class_='location').text.strip())
                except:
                    Location.insert(0, '')
                
                #Description
                Jobdescription.insert(0, '')
                
                #Date
                try:
                    posteddatelist.insert(0, listofjobsvar[l].find('span', class_='date').text.strip())
                except:
                    posteddatelist.insert(0, '') 
                    
                try:
                    if re.findall("hours", listofjobsvar[l].find('span', class_='date').text.strip()):
                        jobposteddate.insert(0, datetime.today().strftime('%Y-%m-%d'))
                    else:
                        number = listofjobsvar[l].find('span', class_='date').text
                        number = number.split(' ')[0].strip()
                        jobposteddate.insert(0, (datetime.today() - timedelta(days=int(number))).strftime('%Y-%m-%d'))
                except:
                    
                    jobposteddate.insert(0, '')   
               
                try:
                    JobCategory.insert(0, v)
                except:
                    JobCategory.insert(0, '')
                
                
                #Date scraped
                CurrentDatDate = date.today().strftime('%m/%d/%Y')
                DateScraped.insert(0, CurrentDatDate)
                print(len(CompanyFinalLink))
    else:
        for pages in range(0, CatFinalPages * 10, 10):
            #process this
            print("SP LT "+ str(pages))
            
            if Pausecounter <= Indeedcounterlimit:
                time.sleep(0.2)
            else:
                Indeedcounterlimit = Indeedcounterlimit + 100000
                print("sleeping for 5min...")
                #Sleep process for 5 min
                #time.sleep(240)
                time.sleep(10)
    
            Mainurl = u + str(pages)
            
            try:
                html = urllib.request.urlopen(Mainurl)
            except:
                continue
            
            try:
                checkresponse1 = html.geturl()
                html  = urllib.request.urlopen(checkresponse1)
            except:
                continue
            
            ResultsColsoup = BeautifulSoup(html,'html.parser')       
            resultslist = ResultsColsoup.find_all(id = 'resultsCol')
            
            #resultslist.find('div', class_='jobsearch-SerpJobCard unifiedRow row result clickcard')
            
            try:
                listofjobsvar = resultslist[0].findAll('div', class_='jobsearch-SerpJobCard')
            except:
                continue
            
            for l in range(len(listofjobsvar)):
                Pausecounter = Pausecounter + 1
                
                #Job URL
                try:
                    CompanyFinalLink.insert(0, "https://www.indeed.com" + listofjobsvar[l].find('a')['href'].strip())
                except:
                    CompanyFinalLink.insert(0, '')
                
                #Job Title
                try:
                    JobTitle.insert(0, listofjobsvar[l].find('div', class_='title').text.strip())
                except:
                    JobTitle.insert(0, '')
                #Company name
                try:
                    Company.insert(0,listofjobsvar[l].find('span', class_='company').text.strip())
                except:
                    Company.insert(0, '')
                #location
                
                try:
                    try:
                        Location.insert(0, listofjobsvar[l].find('div', class_='location').text.strip())
                    except:
                        Location.insert(0, listofjobsvar[l].find('span', class_='location').text.strip())
                except:
                    Location.insert(0, '')
                
                #Description
                Jobdescription.insert(0, '')
                
                #Date
                try:
                    posteddatelist.insert(0, listofjobsvar[l].find('span', class_='date').text.strip())
                except:
                    posteddatelist.insert(0, '') 
                    
                try:
                    if re.findall("hours", listofjobsvar[l].find('span', class_='date').text.strip()):
                        jobposteddate.insert(0, datetime.today().strftime('%Y-%m-%d'))
                    else:
                        number = listofjobsvar[l].find('span', class_='date').text
                        number = number.split(' ')[0].strip()
                        jobposteddate.insert(0, (datetime.today() - timedelta(days=int(number))).strftime('%Y-%m-%d'))
                        
                except:
                    
                    jobposteddate.insert(0, '')  
               
                try:
                    JobCategory.insert(0, v)
                except:
                    JobCategory.insert(0, '')
                    
                #Date sourced
                CurrentDatDate = date.today().strftime('%m/%d/%Y')
                DateScraped.insert(0, CurrentDatDate) 
                print(len(CompanyFinalLink))          

Indeedendtime = datetime.now().time()  
Indeedendtime1 = datetime.today().strftime('%A')                      


print("Start time is:", Indeedstarttime.strftime('%H:%M:%S') + ' on ' + Indeedstarttime1)
print("End time is:", Indeedendtime.strftime('%H:%M:%S') + ' on ' + Indeedendtime1)


print("Writing to dataframe")
outputForIndeed = pd.DataFrame( columns = ['Index_id','CompanyJobURL', 'Company_Name', 'Job_Title', 'Job_Location','Job_Category','Job_Description', 'DateScraped', 'Job_Posted_Text', 'Job_Posted_Date']) 

outputForIndeed['Index_id']= range(1, len(CompanyFinalLink) + 1 ,1)
outputForIndeed['CompanyJobURL']= CompanyFinalLink
outputForIndeed['Company_Name']= Company
outputForIndeed['Job_Title']=JobTitle
outputForIndeed['Job_Location']= Location
outputForIndeed['Job_Description']= Jobdescription
outputForIndeed['Job_Category']= JobCategory
outputForIndeed['DateScraped']= DateScraped
outputForIndeed['Job_Posted_Text']= posteddatelist
outputForIndeed['Job_Posted_Date']= jobposteddate



#output to csv
#outputForIndeed.to_csv('Dell_Hiring-indeed_0528_1.csv')

#output into database
process_start_time = dt.datetime.now()

import sqlalchemy

params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};server=[servername];database=[database name];uid=[user ID];pwd=[password]") 

engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True) 
engine.connect()


outputForIndeed.to_sql(name="TMP_HIRING_INDEED_USA",con=engine, index=False, if_exists='replace',chunksize = 100000,
                          dtype={'Index_id':       sqlalchemy.types.INTEGER() ,
                                'CompanyJobUrl':  sqlalchemy.types.NVARCHAR(length=3000),
                                'Company_Name':   sqlalchemy.types.NVARCHAR(length=500),
                                'Job_Title':      sqlalchemy.types.NVARCHAR(length=500),
                                'Job_Location':   sqlalchemy.types.NVARCHAR(length=255),
                                'Job_Description': sqlalchemy.types.NVARCHAR(length=10),
                                'Job_Category': sqlalchemy.types.NVARCHAR(length=1000),
                                'DateScraped': sqlalchemy.types.NVARCHAR(length=100),
                                'Job_Posted_Text': sqlalchemy.types.NVARCHAR(length=100),
                                'Job_Posted_date': sqlalchemy.types.NVARCHAR(length=100)
                            })

# End time for Process
process_end_time = dt.datetime.now()
print("Database Process Start Time:= "+process_start_time.strftime("%m/%d/%Y, %H:%M:%S"))
print("Database Process End Time:=   "+process_end_time.strftime("%m/%d/%Y, %H:%M:%S"))
print("Total Database Process Time:= " +str(process_end_time-process_start_time))
#print(output)
#Date =  Date+dt.timedelta(days=1)
##Process End time#


