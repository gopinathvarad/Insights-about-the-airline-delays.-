package exercises

import org.bson.Document


import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.Projections.*
import com.mongodb.client.MongoClients

import static com.mongodb.client.model.Filters.*
import static com.mongodb.client.model.Projections.*
import static com.mongodb.client.model.Sorts.*
import static com.mongodb.client.model.Accumulators.*

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

import static com.mongodb.client.model.Aggregates.*


def startTime = System.currentTimeMillis()
def properties = new Properties()
def propertiesFile = new File('src/main/resources/mongodb.properties')
propertiesFile.withInputStream {
	properties.load(it)
}

// parse JSON file
def jsonFile = new File('src/main/resources/airlines2.json')
def jsonSlurper = new JsonSlurper()
def list = jsonSlurper.parseText(jsonFile.text)

// create connection and upload contents
def mongoClient = MongoClients.create("mongodb+srv://${properties.USN}:${properties.PWD}@${properties.CLUSTER}.${properties.HOST}.mongodb.net/${properties.DB}?retryWrites=true&w=majority");
//def mongoClient = MongoClients.create("mongodb://localhost:27017");
def db = mongoClient.getDatabase("test");
def col = db.getCollection("ARR")
col.drop()

for (obj in list) {
	def doc = Document.parse(JsonOutput.toJson(obj))
	col.insertOne(doc)
}






// ---------------------- Please wait for 50-60 seconds because the data is quite huge  ----------------------

// Simple projection query for the MongoDB Data


import com.mongodb.client.model.Projections
import com.mongodb.client.model.Filters


def projection = Projections.fields(
	Projections.include("Airport"),
	Projections.include("Time"),
	Projections.include("Statistics.# of Delays"),
	Projections.include("Statistics.Carriers"),
	Projections.include("Statistics.Flights"),
	Projections.include("Statistics.Minutes Delayed")
)

def result = col.find().projection(projection)

result.forEach { doc ->
	println(doc.toJson())
}

