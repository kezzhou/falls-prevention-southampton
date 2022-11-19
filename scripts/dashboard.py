#### Imports ####

import pandas as pd
import streamlit as st

#### Title ####

st.title('Southampton Falls Prevention 2022')


#### Dashboard Header and Description ####


df = pd.read_csv('./data/patients.csv')

st.header('Dashboard')

st.caption('Southampton Hospital Fall Prevention Sector has developed a dashboard complete with monthly database updates, patient and EBP program geo tracking, Tableu compatibility, and patient information statistical analysis. The focus demographic of our program is patients 65 and over with a primary or secondary diagnosis of a fall. Once these patients are discharged, our program tracks and contacts them for follow-up debriefing and enrollment in relevant EBP programs based on proximity and relevancy. Only patients who have granted consent and signed the appropriate forms prior to discharge have been contacted.')

### Patient Master List #### 


st.header('Patient Master List')

df = df[['acct', 'mrn', 'svc', 'stay_type', 'last_name',
       'first_name', 'middle_name', 'dob', 'age', 'address1', 'address2',
       'city', 'state', 'zip', 'phone', 'cell', 'ed_arrival', 'admit',
       'discharge', 'er_disposition', 'final_disch_disposition',
       'final_disch_disp_desc', 'pcp_number', 'pcp_name',
       'er_log_chief_complaint', 'cpsi_chief_complaint']]


st.dataframe(df)


#### Evidence-Based Programs ####

st.subheader('Current Southampton Evidence-Based Programs')

df = pd.read_csv('./data/masterlistebp.csv')

df = df[df['status'] == 'current']

df = df[['name', 'attendance', 'links']]

st.dataframe(df)

st.caption('A list of EBPs currently approved by Southampton Falls Prevention')

st.subheader('Suggested Southampton Evidence-Based Programs')

df = pd.read_csv('./data/masterlistebp.csv')

df = df[df['status'] == 'suggested']

df = df[['name', 'attendance', 'links']]

st.dataframe(df)

st.caption('A list of EBPs suggested for approval by Southampton Falls Prevention')


#### Geo Data Patients ####

st.subheader('Patient Geo Data')

df = pd.read_csv('./data/patientgeo.csv')

st.map(df)

df = df[['mrn', 'lat', 'lon']]

if st.checkbox('Show geo data corresponding to patients by MRN'):
    st.dataframe(df)

st.caption('A map of patient addresses to aid in pairing them with appropriate evidence-based programs and communities')


#### EBP Geo Data ####

st.subheader('Evidence-Based Programs Geo Data')

df = pd.read_csv('./data/ebpgeo.csv')

st.map(df)

df = df[['ebp', 'lat', 'lon']]

if st.checkbox('Show geo data corresponding to Evidence-Based Program'):
    st.dataframe(df)

st.caption('A map of EBPs to allow for easy pairing of discharged patients to EBP by proximity data')


#### Barchart - Patients by Age ####

df = pd.read_csv('./data/patients.csv')

df = df['age'].value_counts()

st.subheader('Patients by Age')

st.bar_chart(df)

st.caption("A bar graph representation of patient age according to the dataset")


### Line Chart - Fall Incidents over time ####

st.subheader('Southampton Hospital Fall Admits From 2021 to 2022')

df = pd.DataFrame({
   'admits': [1, 8, 11, 3, 8, 8, 6, 8, 7, 8, 6, 9, 10, 13]
   }, index=['2021/11','2021/12','2022/01', '2022/02','2022/03','2022/04','2022/05','2022/06','2022/07','2022/08','2022/09','2022/10','2022/11','2022/12'])

st.line_chart(df)

st.caption("A line chart displaying admits for falls to Southampton Hospital from 2021 to 2022")