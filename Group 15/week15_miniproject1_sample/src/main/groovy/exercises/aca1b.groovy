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


// Create a list to store the airports with minutes delayed due to security greater than 2000
// query for each airport where minutes delayed due to security is greater than 2000


def airportsWithSecurityDelayGreaterThan500 = []

airlineData.each { entry ->
	def airportCode = entry.Airport.Code
	def securityMinutesDelayed = entry.Statistics["Minutes Delayed"]["Security"]

	if (securityMinutesDelayed > 2000) {
		airportsWithSecurityDelayGreaterThan500 << [Airport: airportCode, SecurityMinutesDelayed: securityMinutesDelayed]
	}
}

// Print the airports with minutes delayed due to security greater than 500
airportsWithSecurityDelayGreaterThan500.each { airportData ->
	println("Airport: ${airportData.Airport}, Security Minutes Delayed: ${airportData.SecurityMinutesDelayed}")
}
