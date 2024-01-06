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





// Please wait for 50 seconds to 1 minute for code execution because the dataset is quite huge
// query to find the flight count by month




try {
	def pipeline = [
		group("\$Time.Month Name", sum("totalFlights", 1)),
		sort(ascending("_id"))
	]

	def cursor = col.aggregate(pipeline)
	def results = cursor.into([])

	if (results.size() > 0) {
		println("Flight count by Month:")
		results.each { result ->
			def monthName = result.get("_id")
			def totalFlights = result.get("totalFlights")
			println("$monthName: $totalFlights flights")
		}
	} else {
		println("No data found.")
	}
} catch (Exception e) {
	println("Error: $e")
}




