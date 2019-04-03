import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sys

class crimeValue(dml.Algorithm):
    contributor = 'asadeg02_gxy9598'
    reads = ['asadeg02_gxy9598.average_Property_value']
    writes = ['asadeg02_gxy9598.crime_value']

    @staticmethod
    def execute(trial = True):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')

        repo.dropCollection('asadeg02_gxy9598.crime_value')
        repo.createCollection('asadeg02_gxy9598.crime_value')
        values = repo.asadeg02_gxy9598.average_Property_value
        crimes = repo.asadeg02_gxy9598.address_crime_rate

        value_x = values.find()
        crime_x = crimes.find()

        # for i in value_x:
        #     print(i["_id"])
        for i in value_x:
            crime_vs_value = {}
            address = i["_id"]
            #print(i["_id"])
            #print(address)
            crime_vs_value["address"] = address
            # print(crime_vs_value["address"])
            crime_vs_value["value"] = i["AverageValue"]
            crime_vs_value["crime_count"] = 0
            crime_x = crimes.find()
            for y in crime_x:
                #print(1)
                if address == str(y['_id']):
                    crime_vs_value["crime_count"] = y['value']
                    #print(y['value'])
                    #print(crime_vs_value["crime_count"])
            #crime_x.ResetReading()
                # else:
                #     crime_vs_value["crime_count"] = 0
            repo['asadeg02_gxy9598.crime_value'].insert_one(crime_vs_value)
            #print(crime_vs_value)
        repo.logout()
        endTime = datetime.datetime.now()
        print("Finish unioning crime and property value")
        return {"start": startTime, "end": endTime}
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/')
        this_script = doc.agent('alg:asadeg02_gxy9598#Crime_Value', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


        resource_crime_value = doc.entity('dat:asadeg02_gxy9598#average_Property_value', {prov.model.PROV_LABEL:'average_Property_value', prov.model.PROV_TYPE:'ont:DataSet'})
        get_crime_value = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get crime value', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_crime_value, this_script)
        doc.usage(get_crime_value, resource_crime_value , startTime)
        crime_value = doc.entity('dat:asadeg02_gxy9598#Crime_Value', {prov.model.PROV_LABEL:'Crime vs value report', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime_value, this_script)
        doc.wasGeneratedBy(crime_value, get_crime_value, endTime)
        doc.wasDerivedFrom(crime_value, resource_crime_value, get_crime_value, get_crime_value, get_crime_value)


crimeValue.execute(True)
doc = crimeValue.provenance()
