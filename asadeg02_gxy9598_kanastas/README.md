# Project Description And Ideas 

T1he Main project we are assigned is a spark project named "South Boston Neighborhood Development" the goal of which is to identify buildings that may not even be on the market yet and then finding contact information of the person/people who own it and permit number of the buildings. Since the initial project doesn't meet the requiemetns for project#1 we decided to define some interesting questions whithin the scope of our main project which could be potentially answered using the datasets given to us by spark. Answring these questions and analysing their answers could also provide a solution for the main project. 

The two quesotions to be answred are the following:

1- Finding information about the buildings/properties in the most dangereous neighborhood of Boston area. These infomation include the value of these porpeties, type of the crimes happening in the neighbourhood, permit number of buildings, owners of the properties,...

2- Clustering food establishments in Boston area using their location and then finding infomation about properties in the most compact cluster (with the most food establishments in it) to see if access to food establishments has any impact on the population living in that area or on the value of properties.

---
Data Sets
---

Permit Database : https://data.boston.gov/api/3/action/datastore_search?resource_id=6ddcd912-32a0-43df-9908-63574f8c7e77&limit=125650

Crime Incident Database : https://data.boston.gov/api/3/action/datastore_search?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b&limit=366640

Active Food Establishments: https://data.boston.gov/api/3/action/datastore_search?resource_id=f1e13724-284d-478c-b8bc-ef042aa5b70b&limit=3010

Street Names: https://data.boston.gov/api/3/action/datastore_search?resource_id=a07cc1c6-aa78-4eb3-a005-dcf7a949249f&limit=18992

**New data sets Added for project2 and spark project:**

**Voter File**: Spark has been able to get the voter file from city hall which contains information about all the people in south Boston who voted. This data set is really important for completing both spark project and proj#2 and proj#3 since it contains personal information of people including their age and occupation and phone number. (the purpose of spark project is to find the contact information of owner of buildings which are not on the market. we have cleaned up this data set and converted it into json format. now this data set is available at: http://datamechanics.io/data/asadeg02/Voter-File.json

please note that the code for cleaning up is not included. it will be included in spark project GitHub repository.

**Property Per Voter**: the addresses in voter file are the voters residency addresses but we are interested in finding first all the properties (Potentially in Boston) they own and then finding some information including value about those properties.
we extracted all the first names and last names from voter file and we used last name + first name the search key to scrap assessors website: https://www.cityofboston.gov/assessing/search which gives us the exact addresses of all buildings/properties in Boston owned people and so merging this data set with voter file data set allows us to have access to both contact information of the owner and address of the their properties. again since scraping is really slow and it took a couple of hours to scrape the Assessors for all the voters in voter file we have done that separately. the code is not included in this repo but will on spark GitHub repository. this data set is now available at: http://datamechanics.io/data/asadeg02/Property-Per-Voter.json

---
Aditional Resources
---

Zillow Search API: "https://www.zillow.com/howto/api/GetSearchResults.htm"

We are using this API to create a database of the property addresses that are on the market which we are not using for project#1 but is required for the main South Boston Neighborhood Development project. 

Assesssors (Assessing online - City Of Boston): https://www.cityofboston.gov/assessing/search/ 

We are scraping this website to find the information we are interested in about the propeties in City Of Boston including the value of propeties. This resource is useful for both project#1 and Main project.

**Please note that scraping a website or calling an API is such a slow process and scraping accessors for all the street addresses is beyond the time and resources available so we have put a limit on the number of addresses we want to scrape accessors for or call the api for**

## Overview Of Transformations 

"aggregateCrimesIncident", "mergeValueWithCrimeRate" and "mergeValueWithPermitAndCrime" are 3 transfomations done to provide an answer for the first question by first aggregating crime incident data set using the "street" attribute as keys and then sorting the result in ascending order then finding the value and ParcelID for the result by scraping `Accsesssors` (we can look at this part of tranfomation as a merge but one of the data sets is in the cloud and not stored in the data base due to time constraints explained) and finally merging the result with crime indincet and permit databases and projecting the desired attributes. Please note that our notion of "the most dangerous" here is the street addresses with the most records in "crime incident report" database.

"foodStablishmentClusters" provides an answer for the second question asked by fisrt clustering food stablishments by ther locations, then counting the number of food stablishments in each cluster, then finding the closest food stablishment center for each building in permit data base) and finally storing the infomation about propeties in the most compact cluster (with the most food stablishments) into a databse.


## Running The Code

In Order to be able to run the code you need to install `selenium` and `xmltodict`. We are using selenium for scraping Accsesssors and using xmltodic for parsing the Zillow Search API responses since they are in xml foramt.

For scraping Accsesssors in scrapeBostonGov method in "mergeValueWithCrimeRate.py" module, you need to specify the path to your chrome web driver. The path already used, is the default path when you install chrome web driver. 
