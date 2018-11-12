package demo3

import demo2.setStringValueOnCell
import demo2.writeFile
import org.apache.poi.xssf.usermodel.XSSFRow
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.File
import java.util.*
import kotlin.collections.HashMap

const val workingFolder = "/Users/atellez/Documents/To-Do/redeliveryBug/FindDuplicateRequestMarkers/"
const val fileInExtension = ".csv"
const val fileOutExtension = ".xlsx"
const val fileName = "unitRequestMarkerTaxRules"

const val excelReportByUnitFileName = "affectedUnits_03-10-2018_09-11-2018"
const val excelReportByStateFileName = "affectedUnitsByState_03-10-2018_09-11-2018"
fun getDefaultFullXLSXFilePath(fileName: String) = workingFolder + fileName + fileOutExtension

var totalLines = 0


//Store successfully subscribed query result
var uniqueUnits = HashSet<String>()
val metaMap4 = HashMap<String, UnitMetaData>()


var totallyAffectedUnits: Map<String, UnitMetaData> = HashMap()
var unitsWithRepeatedAndEmptyRequests: Map<String, UnitMetaData> = HashMap()
var unitsWithRepeatedRequests: Map<String, UnitMetaData> = HashMap()
var unitsWithEmptyRequests: Map<String, UnitMetaData> = HashMap()


fun main(args: Array<String>) {
//    approach2()
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
    completeMetaData()
    writeReports4()
}

fun readFileLineByLineUsingForEachLine4() {
    val file = File(workingFolder + fileName + fileInExtension)
    val bufferedReader = file.bufferedReader()

    //Skip log headers
    bufferedReader.readLine()

    bufferedReader.forEachLine {
        val tokens = it.split(",")
        if (tokens.isNotEmpty()) {

//            val unitWithRequestMarkerKey = tokens[0] + tokens[1]
            val unit = tokens[0]

            val requestMarker = tokens[1]
            val taxRulesStringBuilder = StringBuilder()
            tokens.forEachIndexed { index, token ->
                if (index > 1) {
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
                unitMetaData.requestMarkerOccurences = HashMap()
            }

            //Set requestMarker occurrence
            if (unitMetaData.requestMarkerOccurences!![requestMarker] == null) {
                unitMetaData.requestMarkerOccurences!![requestMarker] = 0
            }
            unitMetaData.requestMarkerOccurences!![requestMarker] = (1 + unitMetaData.requestMarkerOccurences!![requestMarker]!!)
                    .toByte()
            //Set Rules
            if (rawTaxRules.length > 5) {
                if (unitMetaData.metaListOfTaxRuleJurisdictions == null) {
                    unitMetaData.metaListOfTaxRuleJurisdictions = LinkedList()
                }
                unitMetaData.metaListOfTaxRuleJurisdictions!!.add(extractAllJurisdictions(rawTaxRules))
            } else {
                unitMetaData.hasEmptyRequests = true
            }
            metaMap4[unit] = unitMetaData
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

fun completeMetaData() {
    unitsWithEmptyRequests = metaMap4.filter { it.value.hasEmptyRequests }
    unitsWithRepeatedRequests = metaMap4.filter { it.value.requestMarkerOccurences!!.values.max()!! > 1 }
    unitsWithRepeatedAndEmptyRequests = metaMap4.filter {
        it.value.hasEmptyRequests && it.value.requestMarkerOccurences!!.values.max()!! > 1
    }
    totallyAffectedUnits = metaMap4.filter {
        it.value.hasEmptyRequests || it.value.requestMarkerOccurences!!.values.max()!! > 1
    }

    println("Total requests: $totalLines")
    println("Unique units: ${metaMap4.keys.size}")
    println("Units with repeated requests: ${unitsWithRepeatedRequests.size}")
    println("Units with empty requests: ${unitsWithEmptyRequests.size}")
    println("Units with repeated and empty requests: ${unitsWithRepeatedAndEmptyRequests.size}")
    println("Totally affected units: ${totallyAffectedUnits.size}")

    totallyAffectedUnits.forEach { unitUrl, unitMetaData ->
        unitMetaData.state = extractJurisdictionNameFromLevel(unitMetaData, "STATE")
        unitMetaData.district = extractJurisdictionNameFromLevel(unitMetaData, "DISTRICT")
        unitMetaData.county = extractJurisdictionNameFromLevel(unitMetaData, "COUNTY")
        unitMetaData.city = extractJurisdictionNameFromLevel(unitMetaData, "CITY")
    }
}


fun writeReports4() {
    writeByUnitExcelReport()
    writeByStateExcelReport()

}

fun writeByStateExcelReport() {
    //GroupByState
    val groupBy = totallyAffectedUnits.values.groupBy { it.state }

    val excelReport = XSSFWorkbook()
    val sheet = excelReport.createSheet()

    var i = 0
    writeByUnitExcelReportHeaders(sheet.getRow(0) ?: sheet.createRow(0))
    groupBy.forEach { city, units ->
        units.forEach {unitMetaData ->
            val row = sheet.getRow(i + 1) ?: sheet.createRow(i + 1)

            val unit = setStringValueOnCell(
                    value = unitMetaData.unitUrl,
                    cell = row.getCell(0) ?: row.createCell(0),
                    row = row,
                    index = 0)
            val hasEmpty = setStringValueOnCell(
                    value = unitMetaData.hasEmptyRequests.toString(),
                    cell = row.getCell(1) ?: row.createCell(1),
                    row = row,
                    index = 1)
            val countryJurisdiction = setStringValueOnCell(
                    value = unitMetaData.country,
                    cell = row.getCell(2) ?: row.createCell(2),
                    row = row,
                    index = 2)
            val stateJurisdiction = setStringValueOnCell(
                    value = unitMetaData.state,
                    cell = row.getCell(3) ?: row.createCell(3),
                    row = row,
                    index = 3)
            val districtJurisdiction = setStringValueOnCell(
                    value = unitMetaData.district,
                    cell = row.getCell(4) ?: row.createCell(4),
                    row = row,
                    index = 4)
            val countyJurisdiction = setStringValueOnCell(
                    value = unitMetaData.county,
                    cell = row.getCell(5) ?: row.createCell(5),
                    row = row,
                    index = 5)
            val cityJurisdiction = setStringValueOnCell(
                    value = unitMetaData.city,
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
    val hasEmpty = setStringValueOnCell(
            value = "hasEmptyRequests",
            cell = row.getCell(1) ?: row.createCell(1),
            row = row,
            index = 1)
    val countryJurisdiction = setStringValueOnCell(
            value = "COUNTRY",
            cell = row.getCell(2) ?: row.createCell(2),
            row = row,
            index = 2)
    val stateJurisdiction = setStringValueOnCell(
            value = "STATE",
            cell = row.getCell(3) ?: row.createCell(3),
            row = row,
            index = 3)
    val districtJurisdiction = setStringValueOnCell(
            value = "DISTRICT",
            cell = row.getCell(4) ?: row.createCell(4),
            row = row,
            index = 4)
    val countyJurisdiction = setStringValueOnCell(
            value = "COUNTY",
            cell = row.getCell(5) ?: row.createCell(5),
            row = row,
            index = 5)
    val cityJurisdiction = setStringValueOnCell(
            value = "CITY",
            cell = row.getCell(6) ?: row.createCell(6),
            row = row,
            index = 6)
}

fun writeByUnitExcelReport() {
    val excelReport = XSSFWorkbook()
    val sheet = excelReport.createSheet()

    var i = 0
    writeByUnitExcelReportHeaders(sheet.getRow(0) ?: sheet.createRow(0))
    totallyAffectedUnits.forEach { unitUrl, unitMetaData ->
        val row = sheet.getRow(i + 1) ?: sheet.createRow(i + 1)

        val unit = setStringValueOnCell(
                value = unitUrl,
                cell = row.getCell(0) ?: row.createCell(0),
                row = row,
                index = 0)

        val hasEmpty = setStringValueOnCell(
                value = unitMetaData.hasEmptyRequests.toString(),
                cell = row.getCell(1) ?: row.createCell(1),
                row = row,
                index = 1)
        val countryJurisdiction = setStringValueOnCell(
                value = unitMetaData.country,
                cell = row.getCell(2) ?: row.createCell(2),
                row = row,
                index = 2)
        val stateJurisdiction = setStringValueOnCell(
                value = unitMetaData.state,
                cell = row.getCell(3) ?: row.createCell(3),
                row = row,
                index = 3)
        val districtJurisdiction = setStringValueOnCell(
                value = unitMetaData.district,
                cell = row.getCell(4) ?: row.createCell(4),
                row = row,
                index = 4)
        val countyJurisdiction = setStringValueOnCell(
                value = unitMetaData.county,
                cell = row.getCell(5) ?: row.createCell(5),
                row = row,
                index = 5)
        val cityJurisdiction = setStringValueOnCell(
                value = unitMetaData.city,
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