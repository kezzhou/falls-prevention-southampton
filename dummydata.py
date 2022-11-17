#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#### Imports ####

import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from faker import Faker
import uuid
import random

## uuid and random are native packages, but faker must be installed locally via pip
## consult requirements.txt for more information


#### Create connection with Azure for MySql Database with Dotenv ####

load_dotenv()

AZURE_MYSQL_HOSTNAME = os.getenv("AZURE_MYSQL_HOSTNAME")
AZURE_MYSQL_USER = os.getenv("AZURE_MYSQL_USERNAME")
AZURE_MYSQL_PASSWORD = os.getenv("AZURE_MYSQL_PASSWORD")
AZURE_MYSQL_DATABASE = os.getenv("AZURE_MYSQL_DATABASE")


#connection_string_azure = f'mysql+pymysql://{AZURE_MYSQL_USER}:{AZURE_MYSQL_PASSWORD}@{AZURE_MYSQL_HOSTNAME}:3306/{AZURE_MYSQL_DATABASE}'
#db_azure = create_engine(connection_string_azure)

db_azure = create_engine(connection_string_azure)



#### Check Tables ####

print(db_azure.table_names()) ## we should see 3: patients, geo, ebp_geo



#### Inserting dummy data into newly created tables ####


## Patients

fake = Faker()

fake_patients = [ {
        ## keep just the first few characters of the uuid depending on specific length
        'acct': str(uuid.uuid4())[:7], 
        'mrn': str(uuid.uuid4())[:6],
        'svc': fake.random_element(elements=('s', 'er', 'm', 'o')),
        'stay_type': fake.random_element(elements=('er', 'op')), 
        'last_name':fake.last_name(), 
        'first_name':fake.first_name(),
        'middle_name': fake.random_element(elements=('L', 'R', 'G', 'J', 'A', 'M', 'V', 'W', 'T', 'D', 'E', 'S', 'P', 'C', '')),
        'dob':(fake.date_between(start_date='-94y', end_date='-65y')).strftime("%Y-%m-%d"),
        'address1': fake.street_address(),
        'city': fake.city(),
        'state': fake.state(),
        'zip':fake.zipcode(),
        'phone':fake.phone_number(),
        'cell':fake.phone_number(),
        'ed_arrival':(fake.date_between(start_date='-1y', end_date='-0y')).strftime("%Y-%m-%d"),
        'discharge':(fake.date_between(start_date='-1y', end_date='-0y')).strftime("%Y-%m-%d"),
        'er_disposition': fake.random_element(elements=('home', 'fasttrac')),
        'final_disch_disp_desc': fake.random_element(elements=('DISCHARGED HOME FROM HOSPITAL FAST TRACK AREA', 'DISCHARGED TO HOME OR SELF CARE (ROUTINE DISCHARGE)', 'DISCHARGED TO SKILLED NURSING FACILITY FOR SKILLED CARE(SNF)', 'DISCH HOME UNDER CARE OF HOME HEALTH AGENCY ANTICIPATING SNF')),
        'pcp_number': str(uuid.uuid4())[:6],
        'pcp_name':fake.name(),
        'er_log_chief_complaint': fake.random_element(elements=('arrythmia', 'seizure', 'fall', '')),
        'cpsi_chief_complaint': fake.random_element(elements=('fall', ''))


} for x in range(100)] ## generate 100 patients

df_fake_patients = pd.DataFrame(fake_patients)
df_fake_patients = df_fake_patients.drop_duplicates(subset=['acct'])
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])


insertQuery = "INSERT INTO patients (acct, mrn, svc, stay_type, last_name, first_name, middle_name, dob, address1, city, state, zip, phone, cell, ed_arrival, discharge, er_disposition, final_disch_disp_desc, pcp_number, pcp_name, er_log_chief_complaint, cpsi_chief_complaint) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" ## %s indicates a dynamic value


for index, row in df_fake_patients.iterrows():
    db_azure.execute(insertQuery, (row['acct'], row['mrn'], row['svc'], row['stay_type'], row['last_name'], row['first_name'], row['middle_name'], row['dob'], row['address1'], row['city'], row['state'], row['zip'],row['phone'], row['cell'], row['ed_arrival'], row['discharge'], row['er_disposition'], row['final_disch_disp_desc'], row['pcp_number'], row['pcp_name'], row['er_log_chief_complaint'], row['cpsi_chief_complaint']))
    print("inserted row: ", index)

df_azure = pd.read_sql_query("SELECT * FROM patients", db_azure)

df_azure ## we'll use df_azure to check that our populated tables went through. we can redefine it repeatedly as we proceed

df_azure.to_csv("./patients.csv")






## Patient Geo Data ##

fake = Faker()

fake_geo = [ {
        'lat': fake.coordinate(center=40.8852, radius=0.01),
        'lon': fake.coordinate(center=-72.3802, radius=0.01)

} for x in range(70)] ## generate 70 coordinates for patients who live close to Southampton

df_fake_geo = pd.DataFrame(fake_geo)
#drop dupe latitudes and longitudes
df_fake_geo = df_fake_geo.drop_duplicates(subset=['lat', 'lon'])


insertQuery = "INSERT INTO geo (lat, lon) VALUES (%s, %s)"


for index, row in df_fake_geo.iterrows():
    db_azure.execute(insertQuery, (row['lat'], row['lon']))
    print("inserted row: ", index)

df_azure = pd.read_sql_query("SELECT * FROM geo", db_azure)

fake_geo = [ {
        'lat': fake.latitude(),
        'lon': fake.longitude()

} for x in range(30)] ## generate 30 coordinates for patients who don't live in the Hamptons

df_fake_geo = pd.DataFrame(fake_geo)
df_fake_geo = df_fake_geo.drop_duplicates(subset=['lat', 'lon'])


insertQuery = "INSERT INTO geo (lat, lon) VALUES (%s, %s)"


for index, row in df_fake_geo.iterrows():
    db_azure.execute(insertQuery, (row['lat'], row['lon']))
    print("inserted row: ", index)

df_azure = pd.read_sql_query("SELECT * FROM geo", db_azure)

df_azure.to_csv('./geo.csv')








#### EBP Geo Data ####

fake = Faker()

fake_ebp_geo = [ {
        'ebp': fake.random_element(elements=('A Matter of Balance', 'Fit & Strong', 'Otago Exercise Program', 'Stay Active & Independent For Life', 'Stepping On', 'Tai Chi for Arthritis')), 
        'lat': fake.coordinate(center=40.806622, radius=0.06),
        'lon': fake.coordinate(center=-73.237512, radius=0.4)

} for x in range(50)] ## generate 50 EBPs

df_fake_ebp_geo = pd.DataFrame(fake_ebp_geo)
## drop duplicate mrns (in the case that there are) because mrns should be unique
df_fake_ebp_geo = df_fake_ebp_geo.drop_duplicates(subset=['lat', 'lon'])


insertQuery = "INSERT INTO ebp_geo (ebp, lat, lon) VALUES (%s, %s, %s)"


for index, row in df_fake_ebp_geo.iterrows():
    db_azure.execute(insertQuery, (row['ebp'], row['lat'], row['lon']))
    print("inserted row: ", index)

df_azure = pd.read_sql_query("SELECT * FROM ebp_geo", db_azure)

df_azure

df_azure.to_csv('./ebpgeo.csv')