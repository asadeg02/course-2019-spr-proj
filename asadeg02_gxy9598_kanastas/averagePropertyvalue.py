import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sys
from bson.code import Code
from mergeValueWithCrimeRate import *
from re import sub
from decimal import Decimal

class averagePropertyValue(dml.Algorithm):
    contributor = 'asadeg02_gxy9598'
    reads = ['asadeg02_gxy9598.Get_Boston_Streets']
    writes = ['asadeg02_gxy9598.average_Property_value']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''

        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')

        repo.dropCollection('asadeg02_gxy9598.average_Property_value')
        repo.createCollection('asadeg02_gxy9598.average_Property_value')
        total_address_info = repo.asadeg02_gxy9598.Get_Boston_Streets.find()
        record_addrs = []
        for record in total_address_info:
            averagePropertyvalue = {}
            addr = str(record['ST_NAME']) + " " + str(record['ST_TYPE'])
            print(addr)
            addr = addr.upper()
            print(addr)
            averagePropertyvalue["_id"] = addr
            totalvalue = 0
            record_addr = []
            if addr not in record_addrs and str(record['MUN_L']) == "Boston" and str(record['ST_TYPE']) != "None":
                record_addrs.append(addr)
                record_addr.append(addr)
                residentInfos = mergeValueWithCrimeRate.scrapeBostonGov(record_addr)
                for residentinfo in residentInfos:
                    #print(residentinfo)
                    value = float(Decimal(sub(r'[^\d.]', '', residentinfo['VALUE'])))
                    totalvalue = totalvalue + value
                #print(totalvalue)
                output = 0
                if totalvalue != 0 or len(residentInfos) != 0:
                    output = totalvalue / len(residentInfos)
                averageValue = round(output, 2)
                #print(averageValue)
                averagePropertyvalue["AverageValue"] = averageValue
                #print(averagePropertyvalue)
                repo['asadeg02_gxy9598.average_Property_value'].insert_one(averagePropertyvalue)
                print("Load average property value by street")
        repo["asadeg02_gxy9598.average_Property_value"].metadata({'complete':True})
        print(repo["asadeg02_gxy9598.average_Property_value"].metadata())
        print("Finish calculating average property value")

        repo.logout()
        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}
        #record_addrs.sort()
        #residentInfosD = mergeValueWithCrimeRate.scrapeBostonGov(record_addrs)
        #residentInfos = []
        # print (len(residentInfosD))
        # for ri in residentInfosD:
        #     if ri not in residentInfos:
        #         residentInfos.append(ri)
        # for t in residentInfos:
        #     if t['']
    @staticmethod
    def execute(trial = True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')
        repo.dropCollection('asadeg02_gxy9598.average_Property_value')
        repo.createCollection('asadeg02_gxy9598.average_Property_value')
        total_address_info = repo.asadeg02_gxy9598.Get_Boston_Streets.find()
        record_addrs = []
        for record in total_address_info[:15]:
            averagePropertyvalue = {}
            addr = str(record['ST_NAME']) + " " + str(record['ST_TYPE'])
            addr = addr.upper()
            averagePropertyvalue["_id"] = addr
            totalvalue = 0
            record_addr = []
            if addr not in record_addrs and str(record['MUN_L']) == "Boston" and str(record['ST_TYPE']) != "None":
                record_addrs.append(addr)
                record_addr.append(addr)
                residentInfos = mergeValueWithCrimeRate.scrapeBostonGov(record_addr)
                for residentinfo in residentInfos:
                    #print(residentinfo)
                    value = float(Decimal(sub(r'[^\d.]', '', residentinfo['VALUE'])))
                    totalvalue = totalvalue + value
                #print(totalvalue)
                output = 0
                if totalvalue != 0 or len(residentInfos) != 0:
                    output = totalvalue / len(residentInfos)
                averageValue = round(output, 2)
                #print(averageValue)
                averagePropertyvalue["AverageValue"] = averageValue
                #print(averagePropertyvalue)
                repo['asadeg02_gxy9598.average_Property_value'].insert_one(averagePropertyvalue)
                print("Load average property value by street")
        repo["asadeg02_gxy9598.average_Property_value"].metadata({'complete':True})
        print(repo["asadeg02_gxy9598.average_Property_value"].metadata())
        print("Finish calculating average property value")

        repo.logout()
        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}





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
        this_script = doc.agent('alg:asadeg02_gxy9598#average_Property_Value', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_average_Property_value = doc.entity('dat:asadeg02_gxy9598#Get_Boston_Streets', {prov.model.PROV_LABEL:'Get_Boston_Streets', prov.model.PROV_TYPE:'ont:DataSet'})
        get_average_Property_value = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get average property value', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_average_Property_value, this_script)
        doc.usage(get_average_Property_value, resource_average_Property_value, startTime)
        average_Property_value = doc.entity('dat:asadeg02_gxy9598#average_Property_Value', {prov.model.PROV_LABEL:'Property value report', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(average_Property_value, this_script)
        doc.wasGeneratedBy(average_Property_value, get_average_Property_value, endTime)
        doc.wasDerivedFrom(average_Property_value, resource_average_Property_value, get_average_Property_value, get_average_Property_value, get_average_Property_value)


        repo.logout()
        return doc




averagePropertyValue.execute(True)
doc = averagePropertyValue.provenance()
