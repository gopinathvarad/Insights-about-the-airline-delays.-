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



 
 
 //  groovy query to add all the number of delays caused by carrier, late aircraft , national aviation system, security and weather in each year separately and after that sort them in descending order in terms of the reasons which caused the most delays
 // Initialize maps to store the total delays for each reason by year
 def totalDelaysByYear = [:]
 
 airlineData.each { entry ->
 def year = entry.Time.Year
 
 // Initialize the total delays for this year if it doesn't exist
 if (!totalDelaysByYear[year]) {
	 totalDelaysByYear[year] = [
		 "Carrier": 0,
		 "Late Aircraft": 0,
		 "National Aviation System": 0,
		 "Security": 0,
		 "Weather": 0
	 ]
 }
 
 // Update the total delays for each reason
 totalDelaysByYear[year].Carrier += entry.Statistics["# of Delays"].Carrier
 totalDelaysByYear[year]["Late Aircraft"] += entry.Statistics["# of Delays"]["Late Aircraft"]
 totalDelaysByYear[year]["National Aviation System"] += entry.Statistics["# of Delays"]["National Aviation System"]
 totalDelaysByYear[year].Security += entry.Statistics["# of Delays"].Security
 totalDelaysByYear[year].Weather += entry.Statistics["# of Delays"].Weather
 }
 
 // Sort and print the results for each year
 totalDelaysByYear.each { year, reasons ->
 def sortedReasons = reasons.sort { -it.value }
 
 println("Year: $year")
 sortedReasons.each { reason, totalDelays ->
	 println("$reason: $totalDelays")
 }
 println("-------------")
 }
 
 
 
 
 