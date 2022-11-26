#!/usr/bin/env python3
# -*- coding: utf-8 -*-


#### Imports ####

import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os



#### Create connection with Azure for MySql Database with Dotenv ####

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USERNAME = os.getenv("MYSQL_USERNAME")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

#connection_string_azure = f'mysql+pymysql://{AZURE_MYSQL_USER}:{AZURE_MYSQL_PASSWORD}@{AZURE_MYSQL_HOSTNAME}:3306/{AZURE_MYSQL_DATABASE}'
#db_azure = create_engine(connection_string_azure)

connection_string_azure = f'mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:3306/falls_prevention'
db_azure = create_engine(connection_string_azure)





#### Check Tables ####

print(db_azure.table_names()) ## at this point, there should be no tables until we run the mysql commands below, unless we are doing a rerun of the script



#### Drop Old Tables ####

## in the case that there are existing tables such as during a rerun of the script, this function will drop all tables indiscriminately
## there is no if function that would vet tables to drop.

def droppingFunction_all(dbList, db_source):
    for table in dbList:
        db_source.execute(f'drop table {table}')
        print(f'dropped table {table} succesfully!')
    else:
        print(f'task completed')

droppingFunction_all(db_azure.table_names(), db_azure) ## Dropping function


print(db_azure.table_names())

#### Creating table ####

## Patients

create_table_patients = """
create table if not exists patients (
    id int auto_increment,
    acct varchar(255) null unique,
    mrn varchar(255) null unique,
    svc varchar(255) default null,
    stay_type varchar(255) default null,
    last_name varchar(255) default null,
    first_name varchar(255) default null,
    middle_name varchar(255) default null,
    dob varchar(255) default null,
    age varchar(255) default null,
    gender varchar(255) default null,
    address1 varchar(255) default null,
    address2 varchar(255) default null,
    city varchar(255) default null,
    state varchar(255) default null,
    zip varchar(255) default null,
    phone varchar(255) default null,
    cell varchar(255) default null,
    ed_arrival varchar(255) default null,
    admit varchar(255) default null,
    discharge varchar(255) default null,
    er_disposition varchar(255) default null,
    final_disch_disposition varchar(255) default null,
    final_disch_disp_desc varchar(255) default null,
    pcp_number varchar(255) default null,
    pcp_name varchar(255) default null,
    er_log_chief_complaint varchar(255) default null,
    cpsi_chief_complaint varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

## Raw Geo Location Data

create_table_patient_geo = """
create table if not exists patient_geo (
    id int auto_increment,
    mrn varchar(255) null unique,
    lat float( 15, 10 ) default null,
    lon float( 15, 10 ) default null,
    PRIMARY KEY (id)
); 
"""
# float has parameters (M, D) with M being the total number of digits, and D being the number of digits that come after the decimal point.


## Patient Location Data

create_table_ebp_geo = """
create table if not exists ebp_geo (
    id int auto_increment,
    ebp varchar(255) default null,
    lat float( 15, 10 ) default null,
    lon float( 15, 10 ) default null,
    PRIMARY KEY (id)
); 
"""

create_table_patient_consent = """
create table if not exists patient_consent (
    id int auto_increment,
    mrn varchar(255) null unique,
    last_name varchar(255) default null,
    first_name varchar(255) default null,
    middle_name varchar(255) default null,
    phone varchar(255) default null,
    cell varchar(255) default null,
    consent_given varchar(255) default null,
    PRIMARY KEY (id)
); 
"""

create_table_patient_tracker = """
create table if not exists patient_tracker (
    id int auto_increment,
    mrn varchar(255) null unique,
    last_name varchar(255) default null,
    first_name varchar(255) default null,
    middle_name varchar(255) default null,
    phone varchar(255) default null,
    cell varchar(255) default null,
    ncoa varchar(255) default null,
    ebp varchar(255) default null,
    ebp_address varchar(255) default null,
    discharge varchar(255) default null,
    3_month_date varchar(255) default null,
    3_month_contact_made varchar(255) default null,
    6_month_date varchar(255) default null,
    6_month_contact_made varchar(255) default null,
    9_month_date varchar(255) default null,
    9_month_contact_made varchar(255) default null,
    12_month_date varchar(255) default null,
    12_month_contact_made varchar(255) default null,
    PRIMARY KEY (id)
); 
"""

## we use null unique to signify that there should be no repeating values
## this will be prevalent with unique identifiers, whether it's drug codes or social determinant codes
## we use default null to signify that the default value for an empty cell is null




#### Execute our written commands with Python ####

db_azure.execute(create_table_patients)
db_azure.execute(create_table_patient_geo)
db_azure.execute(create_table_ebp_geo)
db_azure.execute(create_table_patient_consent)
db_azure.execute(create_table_patient_tracker)



print(db_azure.table_names()) ## we can check if our tables went through successfully