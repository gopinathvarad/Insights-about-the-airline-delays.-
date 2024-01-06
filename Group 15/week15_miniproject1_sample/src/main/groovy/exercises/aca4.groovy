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
def properties = new Properties()
def propertiesFile = new File('src/main/resources/mongodb.properties')
propertiesFile.withInputStream {
	properties.load(it)
}

// parse JSON file
def jsonFile = new File('src/main/resources/airlines.json')
def jsonSlurper = new JsonSlurper()
def list = jsonSlurper.parseText(jsonFile.text)

// create connection and upload contents
def mongoClient = MongoClients.create("mongodb+srv://${properties.USN}:${properties.PWD}@${properties.CLUSTER}.${properties.HOST}.mongodb.net/${properties.DB}?retryWrites=true&w=majority");
//def mongoClient = MongoClients.create("mongodb://localhost:27017");
def db = mongoClient.getDatabase("test");
def col = db.getCollection("ARR")





import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import org.bson.Document




//---------------------------------------------------------

/*
// With Pagination the below code fetches the data from the mongoDB 
def databaseName = "test"
def collectionName = "Airline"
def database = mongoClient.getDatabase(databaseName)
def collection = database.getCollection(collectionName)


def startTime = System.currentTimeMillis()


// Define your pagination parameters
def pageSize = 10  // Number of documents per page
def pageNumber = 1  // Page number

// Calculate the skip and limit values based on page number and page size
def skip = (pageNumber - 1) * pageSize
def limit = pageSize

// Define your query filter if needed
def filter = Filters.eq("Airport.Code", "ATL") // Example filter, you can adjust it as needed

// Perform the find operation with pagination
def cursor = collection.find(filter)
					  .skip(skip)
					  .limit(limit)
					  .iterator()

// Iterate over the results and process them
while (cursor.hasNext()) {
	def document = cursor.next()
	// Process the document as needed
	println(document.toJson())
}

// Close the cursor and the MongoDB client when done
cursor.close()
mongoClient.close()

def endTime = System.currentTimeMillis()
def executionTime = endTime - startTime

println "Execution time: $executionTime milliseconds"





*/




// ------------------------------- Please make sure that comment down the code for pagination which is below 56 in order to run the code for without pagination becasue in terminal the whole data cannot be shown

// -------




//without pagination the below code fetches the data form mongoDB


// Create a MongoDB client and connect to your database

def databaseName = "test"
def collectionName = "Airline"
def database = mongoClient.getDatabase(databaseName)
def collection = database.getCollection(collectionName)


def startTime = System.currentTimeMillis()


// Define your query filter if needed
def filter = Filters.eq("Airport.Code", "ATL") // Example filter, you can adjust it as needed

// Perform the find operation without pagination
def cursor = collection.find(filter).iterator()

// Iterate over all the results and process them
while (cursor.hasNext()) {
	def document = cursor.next()
	// Process the document as needed
	println(document.toJson())
}

// Close the cursor and the MongoDB client when done
cursor.close()
mongoClient.close()


def endTime = System.currentTimeMillis()
def executionTime = endTime - startTime

println "Execution time: $executionTime milliseconds"







