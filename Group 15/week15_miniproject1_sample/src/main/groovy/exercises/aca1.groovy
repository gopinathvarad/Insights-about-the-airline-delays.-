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







//Query to print all the different Unique Airlines
// Create a Set to store the unique airline names



def uniqueAirlines = new HashSet()

airlineData.each { entry ->
    def carriers = entry.Statistics.Carriers.Names.split(',')
    
    carriers.each { carrierName ->
        // Add each carrier name to the set
        uniqueAirlines.add(carrierName.trim())
    }
}

// Print the unique airline names
uniqueAirlines.each { airlineName ->
    println(airlineName)
}









