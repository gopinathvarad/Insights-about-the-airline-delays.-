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


// Initialize variables for total weather delays and total minutes delayed due to weather
def totalWeatherDelays = 0
def totalMinutesDelayedWeather = 0

// Project the data and calculate the totals
airlineData.each { entry ->
	def airportCode = entry.Airport.Code
	def airportName = entry.Airport.Name
	def month = entry.Time["Month Name"]
	def year = entry.Time.Year
	def weatherDelays = entry.Statistics["# of Delays"].Weather
	def minutesDelayedWeather = entry.Statistics["Minutes Delayed"].Weather

	// Add the weather delays and minutes delayed due to weather to the totals
	totalWeatherDelays += weatherDelays
	totalMinutesDelayedWeather += minutesDelayedWeather

	// Print information for each airport
	println("Airport: $airportName ($airportCode)")
	println("Month: $month $year")
	println("Weather Delays: $weatherDelays")
	println("Minutes Delayed due to Weather: $minutesDelayedWeather")
	println("------")
}

// Print the totals
println("Total Weather Delays: $totalWeatherDelays")
println("Total Minutes Delayed due to Weather: $totalMinutesDelayedWeather")
