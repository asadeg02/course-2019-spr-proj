import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sys

from math import sqrt

class correlation(dml.Algorithm):
    contributor = 'asadeg02_gxy9598'
    reads = ['asadeg02_gxy9598.crime_value']
    writes = ['asadeg02_gxy9598.correlation']

    @staticmethod
    def execute(trial = True):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')
        repo.dropCollection("asadeg02_gxy9598.correlation")
        repo.createCollection("asadeg02_gxy9598.correlation")

        crime_value = repo.asadeg02_gxy9598.crime_value.find()
        def permute(x):
            shuffled = [xi for xi in x]
            shuffle(shuffled)
            return shuffled

        def avg(x):  # Average
            return sum(x) / len(x)

        def stddev(x):  # Standard deviation.
            m = avg(x)
            return sqrt(sum([(xi - m) ** 2 for xi in x]) / len(x))

        def cov(x, y):  # Covariance.
            return sum([(xi - avg(x)) * (yi - avg(y)) for (xi, yi) in zip(x, y)]) / len(x)

        def corr(x, y):  # Correlation coefficient.
            if stddev(x) * stddev(y) != 0:
                return cov(x, y) / (stddev(x) * stddev(y))
        x = []
        y = []
        corr_crime_value = 0
        for cr in crime_value:
                #print(cr)
                x += [cr["crime_count"]]
                y += [cr["value"]]
        corr_crime_value = corr(x,y)
        #print(corr_crime_value)
        crime_vs_value = {}
        crime_vs_value['name'] = "Crime and property value"
        crime_vs_value['Correlation'] = corr_crime_value
        repo['asadeg02_gxy9598.correlation'].insert_one(crime_vs_value)
        repo.logout()
        endTime = datetime.datetime.now()
        print("Finish calculating correlation value.")
        return {"start": startTime, "end": endTime}
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
        this_script = doc.agent('alg:asadeg02_gxy9598#Correlation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


        resource_correlation = doc.entity('dat:asadeg02_gxy9598#average_crime_value', {prov.model.PROV_LABEL:'crime_value', prov.model.PROV_TYPE:'ont:DataSet'})
        get_correlation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get crime value', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_correlation, this_script)
        doc.usage(get_correlation, resource_correlation , startTime)
        correlation = doc.entity('dat:asadeg02_gxy9598#Correlation', {prov.model.PROV_LABEL:'Correlation report', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(correlation, this_script)
        doc.wasGeneratedBy(correlation, get_correlation, endTime)
        doc.wasDerivedFrom(correlation, resource_correlation, get_correlation, get_correlation, get_correlation)
correlation.execute(True)
doc = correlation.provenance()
