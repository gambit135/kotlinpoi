package e_verifyMaxTimeInLogs

import java.io.File

const val workingFolder = "/Users/atellez/Documents/To-Do/findEmptyTaxRuleMessages/"
const val fileInExtension = ".csv"
const val fileOutExtension = ".xlsx"
const val fileName = "maxTimeWithTaxRulesTest1_b"

const val excelReportByUnitFileName = "affectedUnits_03-10-2018_09-11-2018"
const val excelReportByStateFileName = "affectedUnitsByState_03-10-2018_09-11-2018"
fun getDefaultFullXLSXFilePath(fileName: String) = workingFolder + fileName + fileOutExtension

var totalLines = 0


//Store successfully subscribed query result
var unitsToOccurrenceOriginalMap: HashMap<String, Byte> = HashMap()
var unitsToOccurrenceTimestampMap: HashMap<String, Byte> = HashMap()

var commonUnits :HashSet<String> = HashSet()
var disjointUnits :HashSet<String> = HashSet()

fun main(args: Array<String>) {
    //1. Load units, and occurence of each one for the original "notime" version
    loadTotalUnitOriginalOccurence()

    //2. Then, load units for the log with timestamp field
    loadTimestampUnitOccurence()

    //3.Verify that only one occurence for each unit was loaded on the step 2
    var countOriginalTotal = 0
    println("Total Original Units ${unitsToOccurrenceOriginalMap.keys.size}")
    unitsToOccurrenceOriginalMap
            .filter { it.value > 1 }
            .forEach { unit, count ->
//                println("$unit - $count")
                countOriginalTotal++
            }
    println("Original Units with more than one occurrence: $countOriginalTotal")

    var countTimestampTotal = 0
    println("Total Timestamp Units ${unitsToOccurrenceTimestampMap.keys.size}")
    unitsToOccurrenceTimestampMap
            .filter { it.value > 1 }
            .forEach { unit, count ->
//                println("$unit - $count")
                countTimestampTotal++
            }
    println("Timestamp Units with more than one occurrence: $countTimestampTotal")

    calculateEqualAndDifferentUnits(unitsToOccurrenceOriginalMap, unitsToOccurrenceTimestampMap)
    unitsToOccurrenceTimestampMap
            .forEach { unit, count ->
                println("$unit - $count")
            }

}

fun loadTotalUnitOriginalOccurence() {
    loadUnitOccurenceFromFileToMap(
            fileName = workingFolder + "no" + e_verifyMaxTimeInLogs.fileName + fileInExtension,
            map = unitsToOccurrenceOriginalMap)
}

fun loadTimestampUnitOccurence() {
    loadUnitOccurenceFromFileToMap(
            fileName = workingFolder + e_verifyMaxTimeInLogs.fileName + fileInExtension,
            map = unitsToOccurrenceTimestampMap)
}
fun calculateEqualAndDifferentUnits(map1:HashMap<String,Byte>, map2:HashMap<String, Byte>){
    if (map1.keys.size.equals(map2.keys.size)){
        println("Both sets are the same size")
    }
    if (map2.keys.containsAll(map1.keys)){
        println("Set 2 contains set 1")
    }
    else{
        println("Set 2 DOES NOT CONTAINS set 1")
    }

    if (map1.keys.containsAll(map2.keys)){
        println("Set 1 contains set 2")
    }
    else{
        println("Set 1 DOES NOT CONTAINS set 2")
    }

    if(map1.keys.equals(map2.keys)){
        println("The sets are the same")
    }
    else{
        println("The sets are different")
    }
}

fun loadUnitOccurenceFromFileToMap(fileName: String, map: HashMap<String, Byte>) {
    val file = File(fileName)
    val bufferedReader = file.bufferedReader()
    var linecount = 1
    //Skip log headers
    bufferedReader.readLine()

    bufferedReader.forEachLine {
        val tokens = it.split(",")
        if (tokens.isNotEmpty()) {
            val time = tokens[0]
            val unit = tokens[1]
//            val requestMarker = tokens[2]
//            val taxRulesStringBuilder = StringBuilder()
//            tokens.forEachIndexed { index, token ->
//                if (index > 2) {
//                    taxRulesStringBuilder.append(token)
//                    taxRulesStringBuilder.append(",")
//                }
//            }
            //val rawTaxRules = taxRulesStringBuilder.toString()
            map[unit]
            var unitOccurrence = map[unit]
            if (unitOccurrence == null) {
                unitOccurrence = 0
            }
            map[unit] = (1 + unitOccurrence).toByte()
            linecount++
        }
    }
    println("total lines for " + fileName + ": $linecount")
}
