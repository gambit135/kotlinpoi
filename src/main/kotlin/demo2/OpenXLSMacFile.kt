package demo2

import org.apache.poi.ss.usermodel.*
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.*
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*


const val workingFolder = "/Users/atellez/Documents/To-Do/Subscribe_Me_NAO/"
const val fileExtension = ".xlsx"

const val prefixDateFileName = "1-9_ago_"
const val subscribedUnitsFileName = "${prefixDateFileName}successfully_subscribed"
const val unitsWithSubscriptionRequestFileName = "${prefixDateFileName}subscription_request"
const val postmanOutputFileName = workingFolder + prefixDateFileName + "postmanBodyForSubs"
const val excelReportFileName = "${prefixDateFileName}excelReport"
const val globalOutputFileName = prefixDateFileName + "OUT"


//Store successfully subscribed query result
val unitsAlreadySubscribed: MutableMap<String, UnitToSubscribe> = LinkedHashMap()

//Store to be subscribed query result
val unitsRequestingSubscribe: MutableMap<String, UnitToSubscribe> = LinkedHashMap()

//Store the ones not yet subscribed
val unitsToSubscribe: MutableMap<String, UnitToSubscribe> = LinkedHashMap()

fun main(args: Array<String>) {
    //1. Load units subscribed into memory
    loadDataToMemory(openXLSXFile(getDefaultFullXLSXFilePath(subscribedUnitsFileName)), unitsAlreadySubscribed, false)
    //2. Load units to be subscribed unto memory
    loadDataToMemory(openXLSXFile(getDefaultFullXLSXFilePath(unitsWithSubscriptionRequestFileName)), unitsRequestingSubscribe, false)
    //3. Verify that the units are subscribed. Store the ones that are NOT YET subscribed
    findNotSubscribedUnits()
    //4. Write a text file on the format of the body of the postman request to subscribe them
    writePostmanRequestBody()
    //5. Write an excel spreadsheet with the ones that were subscribed :D
    writeExcelReport()

    println("Units sucessfully subscribed: " + unitsAlreadySubscribed.values.size)
    println("Units requesting subscription: " + unitsRequestingSubscribe.values.size)
    println("Units not subscribed: " + unitsToSubscribe.values.size)
}

fun getDefaultFullXLSXFilePath(fileName: String) = workingFolder + fileName + fileExtension

fun openXLSXFile(fileLocation: String) = XSSFWorkbook(FileInputStream(File(fileLocation)))

fun loadDataToMemory(workbook: Workbook, mapToAddUnits: MutableMap<String, UnitToSubscribe>, isSubscribed: Boolean) {
    val firstSheet = workbook.getSheetAt(0)
    val formatter = DataFormatter()

    for (rowNum in 1..firstSheet.lastRowNum) {
        var cell = firstSheet.getRow(rowNum).getCell(0)

        val time = formatter.formatCellValue(cell)
        val unitUrl = formatter.formatCellValue(firstSheet.getRow(rowNum).getCell(1))

        if (!time.isBlank()) {
            val unitToSubscribe = UnitToSubscribe(
                    unitUrl = unitUrl)
            unitToSubscribe.time = time
            unitToSubscribe.isSubscribed = isSubscribed

            mapToAddUnits[unitUrl] = unitToSubscribe
        }
    }
    unitsAlreadySubscribed.forEach { println(it.toString() + " - " + it.value.time + " - " + it.value.isSubscribed) }
}

fun findNotSubscribedUnits() {
    unitsToSubscribe
            .putAll(unitsRequestingSubscribe
                    .filterNot { unitsAlreadySubscribed.contains(it.key) }
                    .map {
                        it.value.unitUrl to it.value.copy().apply {
                            isSubscribed = false
                            time = it.value.time
                        }
                    }
            )
    println("Units not subscribed are ${unitsToSubscribe.size}: ")
    unitsToSubscribe.forEach { println(it.value.unitUrl) }
//    unitsToSubscribe.forEach { println(it.toString() + " - " + it.value.time + " - " + it.value.isSubscribed) }
}

fun writePostmanRequestBody() {
    var unitsPerPage = 0
    var fileCount = 1
    var totalCount = 0
    var pw = PrintWriter(FileWriter(postmanOutputFileName + fileCount.toString() + ".txt"))
    var pageOfUnits: LinkedList<String>
    pageOfUnits = LinkedList()
    if (unitsToSubscribe.values.size <= 500) {
        println("processing less than 500 to subscribe")
        val urls = unitsToSubscribe.map { "\"${it.value.unitUrl}\"" }
        pw.println(urls.joinToString(prefix = "[", postfix = "]"))
        pw.close()
    } else {
        println("processing MORE than 500 to subscribe")
        println("size of empty list is : " + pageOfUnits.size)
        unitsToSubscribe.values.forEachIndexed { index, element ->
            pageOfUnits.add("\"${element.unitUrl}\"")
            unitsPerPage++
            totalCount++

            if (unitsPerPage == 500 || totalCount == unitsToSubscribe.values.size) {
                pw.println(pageOfUnits.joinToString(prefix = "[", postfix = "]"))
                pw.close()
                //Flush to a new file
                fileCount++
                unitsPerPage = 0
                pw = PrintWriter(FileWriter(postmanOutputFileName + fileCount.toString() + ".txt "))
                pageOfUnits = LinkedList()
            }
        }
        //val urls = unitsToSubscribe.map { "\"${it.value.unitUrl}\"" }
        //pw.println(urls.joinToString(prefix = "[", postfix = "]"))
    }
    pw.close()
}

fun writeFile(workbook: Workbook,
              fileName: String =
                      workingFolder +
                              globalOutputFileName +
                              "_" +
                              LocalDateTime
                                      .now()
                                      .format(DateTimeFormatter
                                              .ofPattern("yyyy-MM-dd HH-mm-ss-SSS")) +
                              fileExtension) {
    try {

        val now = LocalDateTime.now()

        val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm-ss-SSS")
        val formattedDate = now.format(dateFormatter)

        println("Current Date and Time is: $formattedDate")

        val fileOut = FileOutputStream(fileName)
        workbook.write(fileOut)
    } catch (e: Exception) {
        println("Exception: " + e.message)
    }

}

fun setStringValueOnCell(row: Row, index: Int, cell: Cell, value: String = "") =
        (if (cell == null) row.createCell(index) else cell).apply {
            setCellType(CellType.STRING)
            setCellValue(value)
        }!!


fun writeExcelReport() {
    val excelReport = XSSFWorkbook()
    val sheet = excelReport.createSheet()
    var lootCounter = 0
    unitsToSubscribe.values.forEachIndexed { i, element ->
        //println(i)
        val row = sheet.getRow(i + 1) ?: sheet.createRow(i + 1)
        if (lootCounter == 500) {
            lootCounter = 0;
        } else {

            val timeCell = setStringValueOnCell(
                    value = element.time,
                    cell = row.getCell(0) ?: row.createCell(0),
                    row = row,
                    index = 0)
            val urlCell = setStringValueOnCell(
                    value = element.unitUrl,
                    cell = row.getCell(1) ?: row.createCell(1),
                    row = row,
                    index = 1)

            val isSubscribedCell = setStringValueOnCell(
                    value = element.isSubscribed.toString(),
                    cell = row.getCell(2) ?: row.createCell(2),
                    row = row,
                    index = 2)

            lootCounter++
        }
    }

    writeFile(excelReport, getDefaultFullXLSXFilePath(excelReportFileName))
}
