import pandas as pd
import numpy as np

import io
import requests

import warnings
warnings.filterwarnings('ignore')

import os
dir = os.path.dirname(__file__)
base_dir = os.path.join( os.path.dirname( __file__ ), '..' )

############### INITIAL DATA CONNECTION AND LOAD ###############

### Mapping Police Violence: https://mappingpoliceviolence.org/
##### Seems to be a rather comprehensive data source, and connects to the WaPo db - https://mappingpoliceviolence.org/aboutthedata/
mappingUrl = "https://mappingpoliceviolence.org/s/MPVDatasetDownload.xlsx"
mappingFull = pd.read_excel(mappingUrl)

### Washington Post data: https://www.washingtonpost.com/graphics/2018/national/police-shootings-2018/?utm_term=.4a51abb376a0
#### This will be used as a 'secondary' data source, since it only contains shooting victims
waPoUrl  = "https://raw.githubusercontent.com/washingtonpost/data-police-shootings/master/fatal-police-shootings-data.csv"
waPoReq  = requests.get(waPoUrl).content
waPoFull = pd.read_csv(io.StringIO(waPoReq.decode("utf-8")))

### US Census Information: https://www2.census.gov/programs-surveys/popest/datasets/2010-2017/state/asrh/sc-est2017-alldata5.pdf ###
##### Annual State Resident Population Estimates for 5 Race Groups (5 Race Alone or in Combination Groups) by Age, Sex, and Hispanic Origin: April 1, 2010 to July 1, 2017"
census5Url = "https://www2.census.gov/programs-surveys/popest/datasets/2010-2017/state/asrh/sc-est2017-alldata5.csv"
censusFull = pd.read_csv(census5Url)


############### DATA CLEAN-UP & STANDARDIZATION ###############

### CENSUS ###
censusFull['race'] = 'ToBeFilled'
censusFull.loc[(censusFull['race']=='ToBeFilled')&(censusFull['ORIGIN']==2),'race']  = 'Hispanic'
censusFull.loc[(censusFull['race']=='ToBeFilled')&(censusFull['RACE']==1),'race'] = 'White'
censusFull.loc[(censusFull['race']=='ToBeFilled')&(censusFull['RACE']==2),'race'] = 'Black'
censusFull.loc[(censusFull['race']=='ToBeFilled')&(censusFull['RACE']==3),'race'] = 'Native American'
censusFull.loc[(censusFull['race']=='ToBeFilled')&(censusFull['RACE']==4),'race'] = 'Asian'
censusFull.loc[(censusFull['race']=='ToBeFilled')&(censusFull['RACE']==5),'race'] = 'Asian'
censusFull.loc[(censusFull['race']=='ToBeFilled'),'race'] = 'Two or More Races'

# Add abbreviation column
us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY',
}

censusFull['state'] = censusFull['NAME'].map(us_state_abbrev)
censusFull['POPESTIMATE2018'] = censusFull['POPESTIMATE2017'] # No 2018 data as of July 2018, so using 2017 as 2018 estimate

### COMBINE WAPO INTO MAPPING ###
mappingFull = mappingFull[["Victim's name","Victim's age","Victim's gender","Victim's race","URL of image of victim","Date of Incident (month/day/year)","Street Address of Incident","City","State","Zipcode","County","Agency responsible for death","Cause of death","A brief description of the circumstances surrounding the death","Official disposition of death (justified or other)","Criminal Charges?","Link to news article or photo of official document","Symptoms of mental illness?","Unarmed","Alleged Weapon (Source: WaPo)","Alleged Threat Level (Source: WaPo)","Fleeing (Source: WaPo)","Body Camera (Source: WaPo)","WaPo ID (If included in WaPo database)"]]
mappingFull.columns = ["name","age","gender","race","imageUrl","date","address","city","state","zipcode","county","agency","cause","description","officialDisposition","criminalCharges","newsUrl","mentalIllness","unarmed","allegedWeapon","allegedThreatLevel","fleeing","bodyCamera","waPoID"]
mappingFull = mappingFull.drop_duplicates()

### Let's add a year and month column for easier comparisons
mappingFull['year'], mappingFull['month'] = mappingFull['date'].dt.year, mappingFull['date'].dt.month
waPoFull['date'] = pd.to_datetime(waPoFull['date'])
waPoFull['year'], waPoFull['month'] = waPoFull['date'].dt.year, waPoFull['date'].dt.month

### Let's do a little data cleaning/standardizing that'll help us when we want to compare/contrast/combine across data sources
waPoFull.loc[waPoFull['race']=='W','race'] = 'White'
waPoFull.loc[waPoFull['race']=='B','race'] = 'Black'
waPoFull.loc[waPoFull['race']=='H','race'] = 'Hispanic'
waPoFull.loc[waPoFull['race']=='N','race'] = 'Native American'
waPoFull.loc[waPoFull['race']=='A','race'] = 'Asian'
waPoFull.loc[waPoFull['race']=='O','race'] = 'Unknown race' # no category for 'other', so putting in unknown - pretty small grain size here
waPoFull.loc[waPoFull['race']=='None','race'] = 'Unknown race'

waPoFull.loc[waPoFull['manner_of_death']=='shot','manner_of_death'] = 'Gunshot'
waPoFull.loc[waPoFull['manner_of_death']=='shot and Tasered','manner_of_death'] = 'Gunshot, Taser'

waPoFull['armed_detail'] = waPoFull['armed']
waPoFull.loc[-waPoFull['armed'].isin(['unarmed','vehicle','undetermined']),'armed'] = 'Allegedly Armed'
waPoFull.loc[waPoFull['armed']=='unarmed','armed'] = 'Unarmed'
waPoFull.loc[waPoFull['armed']=='vehicle','armed'] = 'Vehicle'
waPoFull.loc[waPoFull['armed']=='undetermined','armed'] = 'Unclear'

### Let's add any missing WaPo data points into our mappingFull dataframe
waPoLeft = pd.merge(waPoFull,mappingFull[['waPoID']],left_on='id',right_on='waPoID',how='left')
waPoLeft = waPoLeft.loc[pd.isna(waPoLeft['waPoID'])]

waPoLeft = waPoLeft[['id', 'name', 'date', 'manner_of_death', 'armed', 'age',
       'gender', 'race', 'city', 'state', 'signs_of_mental_illness',
       'threat_level', 'flee', 'body_camera', 'year', 'month']]
waPoLeft.columns = ['waPoID', 'name', 'date', 'cause', 'unarmed', 'age',
       'gender', 'race', 'city', 'state', 'mentalIllness',
       'allegedThreatLevel', 'fleeing', 'bodyCamera', 'year', 'month']

mappingFull = pd.concat([mappingFull,waPoLeft])


### Let's fill in gaps with WaPo data points
mappingFull = pd.merge(mappingFull,waPoFull[['id','race']],left_on='waPoID',right_on='id',how='left')
mappingFull.loc[(mappingFull['race_x'].str.upper()=='UNKNOWN RACE') & (pd.notna(mappingFull['race_y'])),'race_x'] = mappingFull['race_y']
mappingFull = mappingFull.drop(columns=['id','race_y'])
mappingFull.rename(columns={"race_x": "race"},inplace=True)

mappingFull.loc[mappingFull["race"]=="Pacific Islander","race"] = "Asian"
mappingFull.loc[mappingFull["race"]=="Unknown race","race"] = "Unknown Race"

mappingFull = mappingFull.loc[mappingFull["race"] != "Unknown Race"]
# mappingFull.loc[mappingFull["race"] != "White","race"] = "Non-White"
# censusFull.loc[censusFull["race"] != "White","race"] = "Non-White"

############### DATA CLEAN-UP, STANDARDIZATION, & AGGREGATION ###############

### Police Violence ###
stateYearSumm = mappingFull.groupby(['state','year'])['name'].agg('count').reset_index()
stateYearSumm.rename(columns={"name": "n"},inplace=True)
stateRaceYearSumm = mappingFull.groupby(['state','year','race'])['name'].agg('count').reset_index()
stateRaceYearSumm.rename(columns={"name": "n"},inplace=True)

stateRaceYearSumm = pd.merge(stateRaceYearSumm,stateYearSumm,on=['state','year'])
stateRaceYearSumm.rename(columns={"n_x": "race_deaths", "n_y": "state_deaths"},inplace=True)
stateRaceYearSumm['share_of_deaths'] = stateRaceYearSumm["race_deaths"]/stateRaceYearSumm["state_deaths"]


countryYearSumm = mappingFull.groupby(['year'])['name'].agg('count').reset_index()
countryYearSumm.rename(columns={"name": "n"},inplace=True)
countryRaceYearSumm = mappingFull.groupby(['year','race'])['name'].agg('count').reset_index()
countryRaceYearSumm.rename(columns={"name": "n"},inplace=True)

countryRaceYearSumm = pd.merge(countryRaceYearSumm,countryYearSumm,on=['year'])
countryRaceYearSumm.rename(columns={"n_x":"race_deaths","n_y":"country_deaths"},inplace=True)
countryRaceYearSumm["share_of_deaths"] = countryRaceYearSumm["race_deaths"]/countryRaceYearSumm["country_deaths"]


### Census ###
censusStatePivot   = censusFull.groupby(['NAME','state'])['POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018'].sum().reset_index()
censusStateRacePivot   = censusFull.groupby(['race','NAME','state'])['POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018'].sum().reset_index()
censusCountryPivot = censusFull.groupby(['race'])['POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018'].sum().reset_index()

## State - Year ##
# State and State-Race summary tables #
censusStateYear = pd.melt(censusStatePivot, id_vars=['NAME','state'],value_vars=['POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018'],
                        var_name='year',value_name='population')
censusStateYear['year']=censusStateYear['year'].str[-4:].apply(pd.to_numeric)
censusStateYear.rename(columns={"NAME": "state_full"},inplace=True)

censusStateRaceYear = pd.melt(censusStateRacePivot, id_vars=['race','NAME','state'],value_vars=['POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018'],
                        var_name='year',value_name='population')
censusStateRaceYear['year']=censusStateRaceYear['year'].str[-4:].apply(pd.to_numeric)
censusStateRaceYear.rename(columns={"NAME": "state_full"},inplace=True)

# Metric Calculation #
censusStateRaceYear = pd.merge(censusStateRaceYear,censusStateYear,on=['year','state_full','state'])
censusStateRaceYear.rename(columns={"population_x":"race_population","population_y":"state_population"},inplace=True)
censusStateRaceYear["share_of_population"] = censusStateRaceYear["race_population"]/censusStateRaceYear["state_population"]

# Combination with Police Data summary #
stateRaceYearSumm = pd.merge(stateRaceYearSumm,censusStateRaceYear,on=['year','race','state'],how="left")
stateRaceYearSumm['multiplier'] = stateRaceYearSumm['share_of_deaths']/stateRaceYearSumm['share_of_population']

## Country - Year ##
censusCountryRaceYear = pd.melt(censusCountryPivot, id_vars=['race'],value_vars=['POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018'],
                        var_name='year',value_name='population')
censusCountryRaceYear['year']=censusCountryRaceYear['year'].str[-4:].apply(pd.to_numeric)
censusCountryYear = censusCountryRaceYear.groupby(['year'])['population'].sum().reset_index()

censusCountryRaceYear = pd.merge(censusCountryRaceYear,censusCountryYear,on=['year'])
censusCountryRaceYear.rename(columns={"population_x":"race_population","population_y":"country_population"},inplace=True)
censusCountryRaceYear["share_of_population"] = censusCountryRaceYear["race_population"]/censusCountryRaceYear["country_population"]

# Combination with Police Data summary #
countryRaceYearSumm = pd.merge(countryRaceYearSumm,censusCountryRaceYear,on=['year','race'],how="left")
countryRaceYearSumm['multiplier'] = countryRaceYearSumm['share_of_deaths']/countryRaceYearSumm['share_of_population']
countryYearSumm = pd.merge(countryYearSumm,censusCountryYear,on=['year'],how="left")
countryYearSumm['deaths_per_pop'] = countryYearSumm['n']/countryYearSumm['population']

## Country ##
countryRaceSumm = mappingFull.groupby(['race'])['name'].agg('count').reset_index()
countryRaceSumm.rename(columns={"name": "n"},inplace=True)
countryRaceSumm["country_n"] = countryRaceSumm["n"].sum()
countryRaceSumm["share_of_deaths"] = countryRaceSumm["n"]/countryRaceSumm["country_n"]

censusCountryRace = censusCountryRaceYear.groupby(['race'])['race_population'].sum().reset_index()
censusCountryRace["country_population"] = censusCountryRaceYear['race_population'].sum()
censusCountryRace["share_of_population"] = censusCountryRace["race_population"]/censusCountryRace["country_population"]
censusCountryRace = censusCountryRace[["race","share_of_population"]]

countryRaceSumm = pd.merge(countryRaceSumm,censusCountryRace,on=['race'],how="left")
countryRaceSumm['multiplier'] = countryRaceSumm['share_of_deaths']/countryRaceSumm['share_of_population']

countryRaceSumm.to_json(os.path.join(base_dir,'assets','countryRaceSumm.json'))
