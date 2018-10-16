package demo3

import java.io.File
import java.util.*

const val workingFolder = "/Users/atellez/Documents/To-Do/redeliveryBug/FindDuplicateRequestMarkers/"
const val fileExtension = ".csv"
const val fileName = "DuplicateRequestMarkers"


//Store successfully subscribed query result
//val unitsToPairsMap: MutableMap<String, UnitAndRequestMarkerPair> = LinkedHashMap()
val metaMap = HashMap<String, HashMap<String, Byte>>()
val metaMap2 = HashMap<String, Byte>()
//val metaMap3 = HashMap<String, List<Request>>()
val metaMap3 = HashMap<String, MutableList<String>>()
var uniqueUnits = HashSet<String>()

fun main(args: Array<String>)  {
    approach2()

}
fun extractUnitsFromPairs(unitRequestMarkerPair: String) {

    //Extract Unit only from each joined pair
//    val unitRequestMarkerPair = "\"/units/0000/ad212586-43f3-4fd0-93ea-3ada068ada8a\"2ce1de76eedf"
    val lastIndexOfQuotes = unitRequestMarkerPair.lastIndexOf("\"")
    var unit = unitRequestMarkerPair.substring(0..lastIndexOfQuotes)
    uniqueUnits.add(unit)

}
fun approach1(){
    readFileLineByLineUsingForEachLine1(workingFolder + fileName + fileExtension)



    val filter = metaMap.filter {
        hasDuplicates(it.value).values.isNotEmpty() //If it's not empty -> it has duplicates
    }
    println("" + filter.size + "troubled units")
}
fun approach2(){
    readFileLineByLineUsingForEachLine2(workingFolder + fileName + fileExtension)
    val repeatedPairs = metaMap2.filter{ it.value > 1}
    repeatedPairs.forEach { k, v ->
        extractUnitsFromPairs(k)
    }
    //uniqueUnits.forEach{println(metaMap2[it])}
    println("Duplicate units are: " + uniqueUnits.size)
    println("Total pairs are ${metaMap2.keys.size}")
}
fun approach3(){
    readFileLineByLineUsingForEachLine2(workingFolder + fileName + fileExtension)
    val repeatedPairs = metaMap3.filter{ it.value.size > 1}
    repeatedPairs.forEach { k, v ->
        extractUnitsFromPairs(k)
    }
    //uniqueUnits.forEach{println(metaMap2[it])}
    println("Duplicate units are: " + uniqueUnits.size)
    println("Total pairs are ${metaMap2.keys.size}")
}

fun hasDuplicates(bucket: HashMap<String, Byte>): HashMap<String, Byte> {
    val repeatedRequestMarkersAndCount = HashMap<String, Byte>()
    bucket.forEach { key, value ->
        if (value > 1) {
            println("$key - $value")
            repeatedRequestMarkersAndCount[key] = value
        }
    }
    return repeatedRequestMarkersAndCount
}

fun readFileLineByLineUsingForEachLine2(fileName: String) = File(fileName).forEachLine {
    val tokens = it.split(",")
    if (tokens.isNotEmpty()) {

        val unitWithRequestMarkerKey = tokens[0] + tokens[1]
        var count = metaMap2[unitWithRequestMarkerKey]
        if (count == null) {
            count = 0
        }
        metaMap2[unitWithRequestMarkerKey] = (1 + count).toByte()
    }
}

fun readFileLineByLineUsingForEachLine3(fileName: String) = File(fileName).forEachLine {
    val tokens = it.split(",")
    if (tokens.isNotEmpty()) {

        val unitWithRequestMarkerKey = tokens[0] + tokens[1]
        var jurisdictions: MutableList<String>? = metaMap3[unitWithRequestMarkerKey]
        if (jurisdictions == null) {
            jurisdictions = mutableListOf()
        }
        jurisdictions.add(extractJurisdiction(tokens[2]))
        metaMap3[unitWithRequestMarkerKey] = jurisdictions
    }
}

fun extractJurisdiction(taxRulePayload: String): String {
    //var currentTaxRule = taxRulePayload
    //while(in)
    return "STATE/California,City/LA,County/XXX"
}

fun readFileLineByLineUsingForEachLine1(fileName: String) = File(fileName).forEachLine {
    val tokens = it.split(",")
    if (tokens.isNotEmpty()) {

        println(tokens[0] + " : " + tokens[1])
        val currentUnit = tokens[0]
        val currentRequestMarker = tokens[1]

        var mapOfRequestMarkersToCountsForCurrentUnit = metaMap[currentUnit]
        if (mapOfRequestMarkersToCountsForCurrentUnit == null) {
            mapOfRequestMarkersToCountsForCurrentUnit = HashMap()
            mapOfRequestMarkersToCountsForCurrentUnit[currentRequestMarker] = 1
        } else {
            var currentRequestMarkerCount = mapOfRequestMarkersToCountsForCurrentUnit[currentRequestMarker]
            currentRequestMarkerCount = currentRequestMarkerCount ?: 1
            currentRequestMarkerCount++
            mapOfRequestMarkersToCountsForCurrentUnit[currentRequestMarker] =currentRequestMarkerCount
        }
    }
}

fun readLineWithUnitRequestMarkerAndTaxRules(fileName: String) = File(fileName).forEachLine {
    //    val tokens = it.split(",")
//    if (tokens.isNotEmpty()) {
//        val unit = tokens[0]
//        val requestMarker = tokens[1]
//        val taxaRules = tokens[2]
//
//        var currentRequest = Request()
//
//        var count = metaMap2[unitWithRequestMarker]
//        if (count == null) {
//            count = 0
//        }
//        metaMap2[unitWithRequestMarker] = (1 + count).toByte()
//    }
}