#### Imports ####

import pandas as pd
import streamlit as st
import pgeocode as pg
import plotly_express as px
import base64

#### Title ####

st.title('Southampton Falls Prevention 2022')


#### Dashboard Header and Description ####

st.header('Description')

st.caption('Southampton Hospital Fall Prevention Sector has developed a dashboard complete with monthly database updates, patient and EBP program geo tracking, Tableu compatibility, and patient information statistical analysis. The focus demographic of our program is patients 65 and over with a primary or secondary diagnosis of a fall. Once these patients are discharged, our program tracks and contacts them for follow-up debriefing and enrollment in relevant EBP programs based on proximity and relevancy. Only patients who have granted consent and signed the appropriate forms prior to discharge have been contacted.')

#### Upload your own dataset ####

upload = st.file_uploader(label="Upload your file here", type='csv') ## upload your own csv that changes the analytics



### Patient Master List #### 


st.header('Patient List')

st.caption('Uploaded patient list filtered for patients 65 years of age or older and with an ER Disposition of Home')

if upload is not None:
    df = pd.read_csv(upload)
else:
    df = pd.read_csv('../data/patients.csv')


df = df[df['age'] > 65]

df = df[df['er_disposition'].isin(['fasttrac', 'home'])]

df = df[['acct', 'mrn', 'svc', 'stay_type', 'last_name',
       'first_name', 'middle_name', 'dob', 'age', 'gender', 'address1', 'address2',
       'city', 'state', 'zip', 'phone', 'cell', 'ed_arrival', 'admit',
       'discharge', 'er_disposition', 'final_disch_disposition',
       'final_disch_disp_desc', 'pcp_number', 'pcp_name',
       'er_log_chief_complaint', 'cpsi_chief_complaint']]
## we do this manual filtering for aesthetics - to exclude the default id columns and whatnot

st.dataframe(df)


#### Headers ####
st.header('Analytics')

st.caption('Quick statistics to represent patient data as well as in-depth Tableau analysis')

#### Barchart - Patients by Age ####


df1 = df['age'].value_counts()

st.subheader('Patients by Age')
st.caption("A bar graph representation of patient age according to the dataset")

st.bar_chart(df1)





### Line Chart - Fall Incidents over time ####

st.subheader('Southampton Hospital Fall Admits From 2021 to 2022')

st.caption("A line chart displaying admits for falls to Southampton Hospital from 2021 to 2022")

df['admit'] = pd.to_datetime(df['admit']) # we change the admit dates to datetime format to extract month and date

df['monthyear'] = df['admit'].dt.to_period('M') # we extract month and date into a separate column because we want to classify it by that 

df['monthyear'].sort_values() # sort the values ascending

df = df['monthyear'].value_counts() # count how many cases are in each month of 2021 and 2022 (or other years present in the file)

st.area_chart(df)

## can't get streamlit to recognize the month-year dates as the x-axis values. this may be an issue with value_counts(), but it works fine for the barchart.
## the same problem is present with plotly ex line charts.




### Header and Caption ####

st.header('Tableau')
st.caption('In-depth Tableau analysis with multiple patient parameters')

def show_pdf(file_path):
    with open(file_path,"rb") as f:
        base64_pdf = base64.b64encode(f.read()).decode('utf-8')
    pdf_display = f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="800" height="800" type="application/pdf"></iframe>'
    st.markdown(pdf_display, unsafe_allow_html=True)

show_pdf('../data/tableau.pdf')

with open("../data/tableau.pdf", "rb") as pdf_file:
    PDFbyte = pdf_file.read()

st.download_button(label="Download File", 
        data=PDFbyte,
        file_name="tableau.pdf",
        mime='application/octet-stream')


### Header ###

st.header('Geo Data')
st.caption('Interactive maps with patient and EBP locations')



#### Geo Data Patients ####

st.subheader('Patient Geo Data')
st.caption('A map of patient addresses to aid in pairing them with appropriate evidence-based programs and communities')


df = pd.read_csv('../data/geo.csv')

st.map(df, zoom=12) # we set the default zoom to focus on patients around the Southampton area as per client request

df = pd.read_csv('../data/patientgeo.csv')

df = df[['mrn', 'lat', 'lon']]

if st.checkbox('Show geo data corresponding to patients by MRN'):
    st.dataframe(df)
## checkbox for optionally showing coordinates


#### EBP Geo Data ####

st.subheader('Evidence-Based Programs Geo Data')
st.caption('A map of EBPs to allow for easy pairing of discharged patients to EBP by proximity data')


df = pd.read_csv('../data/ebpgeo.csv')

st.map(df)

df = df[['ebp', 'lat', 'lon']]

if st.checkbox('Show geo data corresponding to Evidence-Based Program'):
    st.dataframe(df)



####Header ####

with st.sidebar:

    st.header('Evidence-Based Programs')

#### Evidence-Based Programs ####

    st.subheader('Current Southampton Evidence-Based Programs')
    st.caption('A list of EBPs currently approved by Southampton Falls Prevention')


    df = pd.read_csv('../data/masterlistebp.csv')

    df = df[df['status'] == 'current']

    df = df[['name', 'attendance', 'links']]

    st.dataframe(df)


    st.subheader('Suggested Southampton Evidence-Based Programs')
    st.caption('A list of EBPs suggested for approval by Southampton Falls Prevention')


    df = pd.read_csv('../data/masterlistebp.csv')

    df = df[df['status'] == 'suggested']

    df = df[['name', 'attendance', 'links']]

    st.dataframe(df)





# Title
st.header("Patient-EBP Pairing System")
st.caption('Pairing tool module that allows user to pair patients with EBPs based on inputs zipcode and optionally EBP name and max distance radius')


# Input bar 1
zipcode = st.number_input("Enter Zip Code", min_value=00000, max_value=99999)
#Set EBP name
ebpName = st.text_input("Enter Name of EBP")
# Set radius
maxRadius = st.slider("Set Radius", 0, 50, 100)


# If button is pressed
if st.button("Submit"):
    
    ## calculate 
    def get_distance(x, y):
        usa_zipcodes = pg.GeoDistance('us')
        distance_in_kms = usa_zipcodes.query_postal_code(x, y) ## this package usefully helps calculate distance between zipcodes in km
        return distance_in_kms

    ## loop through dataframe and calculate distance
    df = pd.read_csv('../data/ebpgeo.csv')

    # df

    df['distance (km)'] = df['zip'].apply(lambda x: get_distance(x, zipcode))
    
    ## sort by distance

    df = df.sort_values(by='distance (km)')

    if maxRadius > 0:
        df=df[df['distance (km)'] <= maxRadius]
        df=df[['ebp', 'zip', 'distance (km)']]
    else:
        df=df[['ebp', 'zip', 'distance (km)']]

    if ebpName == '':
        st.success('Done!')
        st.write(df)
    else:
        df=df[df['ebp'] == ebpName]
        st.success('Done!')
        st.write(df)

