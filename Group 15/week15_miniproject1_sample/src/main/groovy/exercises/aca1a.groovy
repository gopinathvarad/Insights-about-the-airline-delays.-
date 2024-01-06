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





// Create a map to store the total minutes delayed grouped by airport and month

// Create a map to store the total minutes delayed grouped by airport and month
//total minutes delayed in each airport and group it

def totalMinutesDelayedByAirportAndMonth = [:]

airlineData.each { entry ->
	def airportCode = entry.Airport.Code
	def month = entry.Time.Month
	def totalMinutesDelayed = entry.Statistics["Minutes Delayed"]["Total"]

	if (!totalMinutesDelayedByAirportAndMonth[airportCode]) {
		totalMinutesDelayedByAirportAndMonth[airportCode] = [:]
	}
	
	if (!totalMinutesDelayedByAirportAndMonth[airportCode][month]) {
		totalMinutesDelayedByAirportAndMonth[airportCode][month] = 0
	}

	totalMinutesDelayedByAirportAndMonth[airportCode][month] += totalMinutesDelayed
}

// Print the total minutes delayed grouped by airport and month
totalMinutesDelayedByAirportAndMonth.each { airportCode, monthData ->
	println("Airport: $airportCode")
	monthData.each { month, totalMinutes ->
		println("  Month: $month, Total Minutes Delayed: $totalMinutes")
	}
}

