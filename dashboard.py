#### Imports ####

import pandas as pd
import streamlit as st

#### First Dataframe - Head 100 ####

df = pd.read_csv('patients.csv')

#### Header ####

st.header('Southampton Hospital Falls Prevention Dashboard')

#### Caption ####

st.caption('Southampton Hospital Fall Prevention Sector has developed a dashboard complete with monthly database updates, patient and EBP program geo tracking, Tableu compatibility, and patient information statistical analysis. The focus demographic of our program is patients 65 and over with a primary or secondary diagnosis of a fall. Once these patients are discharged, our program tracks and contacts them for follow-up debriefing and enrollment in relevant EBP programs based on proximity and relevancy. Only patients who have granted consent and signed the appropriate forms prior to discharge have been contacted.')

#### Toggle ####

if st.checkbox('Show master list of patients'):
    st.dataframe(df)

#### Geo Data Patients ####
st.map('geo.csv')

#### Barchart - Bite Incidents by Breed ####

#st.subheader('Bite Incidents by Breed')

#st.area_chart())

#st.caption("Here is a simple bar graph representation of bite incident by breed according to the dataset")


#### Line Chart - Bite Incidents over time ####

#st.subheader('Bite Incidents From 2015 to 2017')

#over_time = df['DateOfBite'].value_counts()

#st.line_chart(over_time)

#st.caption("Here is a line chart displaying bite incidents over time")