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





// Initialize a map to track the month with the most delayed flights for each year
// query to find the month in every year which recorded the most delays (Using forEach Method)


def mostDelayedMonthByYear = [:]

airlineData.each { entry ->
    def year = entry.Time.Year
    def month = entry.Time.Month
    def flightsDelayed = entry.Statistics.Flights.Delayed

    // Create a key for the year and month combination (e.g., "2003-06")
    def yearMonth = "${year}-${month}"

    // Initialize the most delayed month if it doesn't exist for this year
    if (!mostDelayedMonthByYear[year]) {
        mostDelayedMonthByYear[year] = [Month: month, TotalDelayedFlights: flightsDelayed]
    } else {
        // Check if this month had more delayed flights than the current most delayed month
        if (flightsDelayed > mostDelayedMonthByYear[year].TotalDelayedFlights) {
            mostDelayedMonthByYear[year] = [Month: month, TotalDelayedFlights: flightsDelayed]
        }
    }
}

// Print the results for each year using the forEach method
mostDelayedMonthByYear.forEach { year, data ->
    println("In the year $year, the month with the most delayed flights is Month ${data.Month}.")
    println("Total delayed flights in that month: ${data.TotalDelayedFlights}")
    println("-------------")
}





















