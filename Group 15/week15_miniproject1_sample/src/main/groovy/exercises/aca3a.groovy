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







// query to find airports with atleast 15 carriers

try {
	// Query to find airports with at least 15 carriers
	def pipeline = [
		unwind("\$Statistics.Carriers.Names"), // Unwind the Carriers.Names array
		group("\$Airport.Code", first("Name", "\$Airport.Name"), sum("totalCarriers", 1)),
		match(gte("totalCarriers", 15)), // Filter airports with at least 15 carriers
		project(fields(include("_id", "Name", "totalCarriers")))
	]

	def cursor = col.aggregate(pipeline)
	def results = cursor.into([])

	if (results.size() > 0) {
		println("Airports with at least 15 carriers:")
		results.each { result ->
			def airportCode = result.get("_id")
			def airportName = result.get("Name")
			def totalCarriers = result.get("totalCarriers")
			println("Airport Code: $airportCode, Airport Name: $airportName, Total Carriers: $totalCarriers")
		}
	} else {
		println("No airports found with at least 15 carriers.")
	}
} catch (Exception e) {
	println("Error: $e")
}