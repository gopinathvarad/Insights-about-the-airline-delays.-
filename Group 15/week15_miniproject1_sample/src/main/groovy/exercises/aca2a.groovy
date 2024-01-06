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




 
 // Group the data by year
 def groupedByYear = airlineData.groupBy { it.Time.Year }
 
 // Initialize a map to track the most delayed reason for each year
 def mostDelayedReasonByYear = [:]
 
 groupedByYear.each { year, entries ->
	 // Create a map to track the reasons and their total counts for each year
	 def reasonCounts = [:]
	 
	 entries.each { entry ->
		 def reasons = entry.Statistics["# of Delays"]
		 
		 reasons.each { reason, count ->
			 if (!reasonCounts[reason]) {
				 reasonCounts[reason] = 0
			 }
			 
			 reasonCounts[reason] += count
		 }
	 }
	 
	 // Find the reason with the most delays for this year
	 def mostDelayedReason = reasonCounts.max { it.value }
	 
	 mostDelayedReasonByYear[year] = mostDelayedReason
 }
 
 // Print the results for each year
 mostDelayedReasonByYear.each { year, mostDelayedReason ->
	 println("In the year $year, the most delayed reason is '${mostDelayedReason.key}' with ${mostDelayedReason.value} delays.")
 }
 
 