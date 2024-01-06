package exercises

import org.bson.Document

import com.mongodb.BasicDBObject
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Updates

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

def jsonSlurper = new JsonSlurper()
def file = new File('src/main/resources/airlines.json')
//def file = new File('src/main/resources/userdetails.json')
def airlineData = jsonSlurper.parseText(file.text)




// Data projection query to select specific information




// Data projection query to select specific information
def projectedData = airlineData.collect { entry ->
	def airportCode = entry.Airport.Code
	def airportName = entry.Airport.Name
	def totalFlightsDelayed = entry.Statistics.Flights.Delayed

	// Projected data structure
	return [Airport: airportCode, Name: airportName, TotalFlightsDelayed: totalFlightsDelayed]
}

// Print the projected data
projectedData.each { entry ->
	println("Airport: ${entry.Airport}")
	println("Name: ${entry.Name}")
	println("Total Flights Delayed: ${entry.TotalFlightsDelayed}")
	println("-------------")
}
