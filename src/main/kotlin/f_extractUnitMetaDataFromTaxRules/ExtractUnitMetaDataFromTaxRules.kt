package f_extractUnitMetaDataFromTaxRules

import b_findUnitsToSuscribe.setStringValueOnCell
import b_findUnitsToSuscribe.writeFile
import org.apache.poi.xssf.usermodel.XSSFRow
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.File
import java.util.*
import kotlin.collections.HashMap

const val workingFolder = "/Users/atellez/Documents/To-Do/extractUnitMetaData/"
const val fileInExtension = ".csv"
const val fileOutExtension = ".xlsx"
const val csvInputFileName = "myLatestAlenia"

const val excelReportByUnitFileName = "AlenyaUnits_27-12-2018_REPORT"
const val excelReportByStateFileName = "AlenyaUnits_27-12-2018_REPORT"
fun getDefaultFullXLSXFilePath(fileName: String) = workingFolder + fileName + fileOutExtension

var totalLines = 0


//Store successfully subscribed query result
var uniqueUnits = HashSet<String>()
val metaMap4 = HashMap<String, UnitMetaData>()
var unitsWithMalformedUrl = HashMap<String, UnitMetaData>()


var totallyAffectedUnits: Map<String, UnitMetaData> = HashMap()
var unitsWithRepeatedAndEmptyRequests: Map<String, UnitMetaData> = HashMap()
var unitsWithRepeatedRequests: Map<String, UnitMetaData> = HashMap()
var unitsWithEmptyRequests: Map<String, UnitMetaData> = HashMap()


fun main(args: Array<String>) {
    approach4()


}

fun extractUnitsFromPairs(unitRequestMarkerPair: String) {

    //Extract Unit only from each joined pair
//    val unitRequestMarkerPair = "\"/units/0000/ad212586-43f3-4fd0-93ea-3ada068ada8a\"2ce1de76eedf"
    val lastIndexOfQuotes = unitRequestMarkerPair.lastIndexOf("\"")
    var unit = unitRequestMarkerPair.substring(0..lastIndexOfQuotes)
    uniqueUnits.add(unit)

}

fun approach4() {
    readFileLineByLineUsingForEachLine4()
    markMalformedUrlUnits()
    completeMetaData()
    writeReports4()
}

fun readFileLineByLineUsingForEachLine4() {
    val file = File(workingFolder + csvInputFileName + fileInExtension)
    val bufferedReader = file.bufferedReader()

    //Skip log headers
    bufferedReader.readLine()

    bufferedReader.forEachLine {
        val tokens = it.split(",")
        if (tokens.isNotEmpty()) {
            var malformedUrlMessage = false
            var unit = tokens[0]
            val lastIndexOfQuotes = unit.lastIndexOf("\"")
            if (lastIndexOfQuotes < 0) {
//                println(it)
            } else {
                unit = unit.substring(1..lastIndexOfQuotes - 1)
                //println(unit)
            }

            val taxRulesStringBuilder = StringBuilder()
            tokens.forEachIndexed { index, token ->
                if (index > 0) {
                    taxRulesStringBuilder.append(token)
                    taxRulesStringBuilder.append(",")
                }
            }
            val rawTaxRules = taxRulesStringBuilder.toString()
//            println("raw: $rawTaxRules")

            var unitMetaData = metaMap4[unit]
            if (unitMetaData == null) {
                unitMetaData = UnitMetaData()
                unitMetaData.unitUrl = unit
                unitMetaData.malformedUrl = malformedUrlMessage
            }


            //Set Rules
            if (rawTaxRules.length > 5) {
                if (unitMetaData.metaListOfTaxRuleJurisdictions == null) {
                    unitMetaData.metaListOfTaxRuleJurisdictions = LinkedList()
                }
                unitMetaData.metaListOfTaxRuleJurisdictions!!.add(extractAllJurisdictions(rawTaxRules))
            }
            if (unit.indexOf("/units/") < 0) {
                unitMetaData.malformedUrl = true
                unitsWithMalformedUrl[unit] = unitMetaData
            } else {
                metaMap4[unit] = unitMetaData
            }
            totalLines++
        }
    }
}

fun extractAllJurisdictions(rawRules: String): LinkedList<Jurisdiction> {
//    println("Extracting all jurisdictions")
//    println("RAW; $rawRules")
    var jurisdictionsForUnit = LinkedList<Jurisdiction>()
    var jurisdictionStringBuilder = StringBuilder(rawRules)

    while (jurisdictionStringBuilder.indexOf("jurisdictionLevel") > -1 &&
            jurisdictionStringBuilder.indexOf("jurisdictionName") > -1) {
        extractJurisdictions(jurisdictionStringBuilder, jurisdictionsForUnit)
    }
//    jurisdictionsForUnit.forEach { println("${it.level} - ${it.name}") }
//    println("no. of rules: " + jurisdictionsForUnit.size)
    return jurisdictionsForUnit
}

fun extractJurisdictions(jurisdictionStringBuilder: StringBuilder, jurisdictions: LinkedList<Jurisdiction>): LinkedList<Jurisdiction> {

    jurisdictionStringBuilder.delete(0, jurisdictionStringBuilder.indexOf("jurisdictionLevel") + 18)
    val jurisdictionLevel = jurisdictionStringBuilder.substring(0, jurisdictionStringBuilder.indexOf(","))

    jurisdictionStringBuilder.delete(0, jurisdictionStringBuilder.indexOf("jurisdictionName") + 17)
    val jurisdictionName = jurisdictionStringBuilder.substring(0, jurisdictionStringBuilder.indexOf(","))

    jurisdictions.add(Jurisdiction(level = jurisdictionLevel, name = jurisdictionName))
    return jurisdictions
}

fun markMalformedUrlUnits(): Unit {
    unitsWithMalformedUrl.forEach { t, _ ->
        metaMap4.forEach {
            //If the current unit contains one malformedUrl unit
            if (it.key.indexOf(t) > -1) {
                it.value.malformedUrl = true
            }
        }
    }
}

fun completeMetaData() {

    println("Total requests: $totalLines")
    println("Unique units: ${metaMap4.keys.size}")
    println("Marlformed URL units: ${unitsWithMalformedUrl.keys.size}")

    metaMap4.forEach { unitUrl, unitMetaData ->
        unitMetaData.state = extractJurisdictionNameFromLevel(unitMetaData, "STATE")
        unitMetaData.district = extractJurisdictionNameFromLevel(unitMetaData, "DISTRICT")
        unitMetaData.county = extractJurisdictionNameFromLevel(unitMetaData, "COUNTY")
        unitMetaData.country = extractJurisdictionNameFromLevel(unitMetaData, "COUNTRY")
        unitMetaData.city = extractJurisdictionNameFromLevel(unitMetaData, "CITY")
    }
}


fun writeReports4() {
    writeByUnitExcelReport()
    writeByStateExcelReport()

}

fun writeByStateExcelReport() {
    //GroupByState
    val groupBy = metaMap4.values.groupBy { it.state }

    val excelReport = XSSFWorkbook()
    val sheet = excelReport.createSheet()

    var i = 0
    writeByUnitExcelReportHeaders(sheet.getRow(0) ?: sheet.createRow(0))
    groupBy.forEach { city, units ->
        units.forEach { unitMetaData ->
            val row = sheet.getRow(i + 1) ?: sheet.createRow(i + 1)

            val unit = setStringValueOnCell(
                    value = unitMetaData.unitUrl,
                    cell = row.getCell(0) ?: row.createCell(0),
                    row = row,
                    index = 0)
            val countryJurisdiction = setStringValueOnCell(
                    value = unitMetaData.country,
                    cell = row.getCell(1) ?: row.createCell(1),
                    row = row,
                    index = 1)
            val stateJurisdiction = setStringValueOnCell(
                    value = unitMetaData.state,
                    cell = row.getCell(2) ?: row.createCell(2),
                    row = row,
                    index = 2)
            val districtJurisdiction = setStringValueOnCell(
                    value = unitMetaData.district,
                    cell = row.getCell(3) ?: row.createCell(3),
                    row = row,
                    index = 3)
            val countyJurisdiction = setStringValueOnCell(
                    value = unitMetaData.county,
                    cell = row.getCell(4) ?: row.createCell(4),
                    row = row,
                    index = 4)
            val cityJurisdiction = setStringValueOnCell(
                    value = unitMetaData.city,
                    cell = row.getCell(5) ?: row.createCell(5),
                    row = row,
                    index = 5)
            val hasMalformedUrl = setStringValueOnCell(
                    value = if (unitMetaData.malformedUrl == true) "true" else "",
                    cell = row.getCell(6) ?: row.createCell(6),
                    row = row,
                    index = 6)
            i++

        }
    }
    writeFile(excelReport, getDefaultFullXLSXFilePath("$excelReportByUnitFileName-byState"))
}

fun writeByUnitExcelReportHeaders(row: XSSFRow) {
    val unit = setStringValueOnCell(
            value = "unit",
            cell = row.getCell(0) ?: row.createCell(0),
            row = row,
            index = 0)
    val countryJurisdiction = setStringValueOnCell(
            value = "COUNTRY",
            cell = row.getCell(1) ?: row.createCell(1),
            row = row,
            index = 1)
    val stateJurisdiction = setStringValueOnCell(
            value = "STATE",
            cell = row.getCell(2) ?: row.createCell(2),
            row = row,
            index = 2)
    val districtJurisdiction = setStringValueOnCell(
            value = "DISTRICT",
            cell = row.getCell(3) ?: row.createCell(3),
            row = row,
            index = 3)
    val countyJurisdiction = setStringValueOnCell(
            value = "COUNTY",
            cell = row.getCell(4) ?: row.createCell(4),
            row = row,
            index = 4)
    val cityJurisdiction = setStringValueOnCell(
            value = "CITY",
            cell = row.getCell(5) ?: row.createCell(5),
            row = row,
            index = 5)
    val hasMalformedUrl = setStringValueOnCell(
            value = "MalformedUrlInMessage",
            cell = row.getCell(6) ?: row.createCell(6),
            row = row,
            index = 6)
}

fun writeByUnitExcelReport() {
    val excelReport = XSSFWorkbook()
    val sheet = excelReport.createSheet()

    var i = 0
    writeByUnitExcelReportHeaders(sheet.getRow(0) ?: sheet.createRow(0))
    metaMap4.forEach { unitUrl, unitMetaData ->
        val row = sheet.getRow(i + 1) ?: sheet.createRow(i + 1)

        val unit = setStringValueOnCell(
                value = unitUrl,
                cell = row.getCell(0) ?: row.createCell(0),
                row = row,
                index = 0)

        val countryJurisdiction = setStringValueOnCell(
                value = unitMetaData.country,
                cell = row.getCell(1) ?: row.createCell(1),
                row = row,
                index = 1)
        val stateJurisdiction = setStringValueOnCell(
                value = unitMetaData.state,
                cell = row.getCell(2) ?: row.createCell(2),
                row = row,
                index = 2)
        val districtJurisdiction = setStringValueOnCell(
                value = unitMetaData.district,
                cell = row.getCell(3) ?: row.createCell(3),
                row = row,
                index = 3)
        val countyJurisdiction = setStringValueOnCell(
                value = unitMetaData.county,
                cell = row.getCell(4) ?: row.createCell(4),
                row = row,
                index = 4)
        val cityJurisdiction = setStringValueOnCell(
                value = unitMetaData.city,
                cell = row.getCell(5) ?: row.createCell(5),
                row = row,
                index = 5)
        val hasMalformedUrl = setStringValueOnCell(
                value = if (unitMetaData.malformedUrl == true) "true" else "",
                cell = row.getCell(6) ?: row.createCell(6),
                row = row,
                index = 6)
        i++
    }


    writeFile(excelReport, getDefaultFullXLSXFilePath(excelReportByUnitFileName))
}


fun extractJurisdictionNameFromLevel(unit: UnitMetaData, level: String): String {
    unit.metaListOfTaxRuleJurisdictions?.forEach { rules ->
        rules.forEach { jurisdiction ->
            if (jurisdiction.level == level) {
                return jurisdiction.name
            }
        }
    }
    return ""
}