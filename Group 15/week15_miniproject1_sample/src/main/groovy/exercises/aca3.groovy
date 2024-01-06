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

// load credentials from src/main/resources/mongodb.properties
// this file should contain
//		USN=yourUsername
//		PWD=yourPassword
//		DATABASE=yourDatabaseName

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


// ----------------------- Please wait for 50 seconds to 1 minute because the data is quite big-----------------------

// query to find the total flights delayed in the year 2004

try {
	
	
	// Define the year for which you want to find delayed flights (2004 in this case)
	def targetYear = 2004

	// Query to find the total delayed flights for the specified year
	def pipeline = [
		match(eq("Time.Year", targetYear)),
		group(null, sum("totalDelayedFlights", "\$Statistics.Flights.Delayed")),  // Use $getField to access nested field
		project(fields(excludeId(), include("totalDelayedFlights")))
	]

	def cursor = col.aggregate(pipeline)
	def result = cursor.first()

	if (result != null) {
		def totalDelayedFlights = result.get("totalDelayedFlights")
		println("Total delayed flights in ${targetYear}: $totalDelayedFlights")
		

		
	} else {
		println("No data found for the specified year.")
	}
} catch (Exception e) {
	println("Error: $e")
} 


def endTime = System.currentTimeMillis()
def executionTime = endTime - startTime

println "Execution time: $executionTime milliseconds"













