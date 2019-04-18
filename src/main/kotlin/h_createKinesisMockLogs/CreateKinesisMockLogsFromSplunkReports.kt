package h_createKinesisMockLogs

import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.PutRecordsRequest
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry
import com.amazonaws.services.kinesis.model.PutRecordsResult
import com.amazonaws.services.kinesis.producer.KinesisProducer
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration
import com.amazonaws.services.kinesis.producer.UserRecordResult
import com.homeaway.pricing.taxes.report.SimpleTaxRule
import java.io.File
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future

val workingFolder = "/Users/atellez/Documents/To-Do/extractUnitMetaData/"
val fileInExtension = ".csv"
val fileOutExtension = ".xlsx"
val csvPpbumInputFileName = "ReceivedTaxRuleMessagesPPBUMs_PROD-2019-04-17"
//val csvPpbumInputFileName = "20190405_PPBUMs"
val csvUpsertedInputFileName = "UpsertedTaxes_PROD-2019-04-17"
//val csvUpsertedInputFileName = "20190405_UPSERTED"
val csvSubscribedInputFileName = "SuccesfullySubscribed_PROD-2019-04-17"
//val csvSubscribedInputFileName = "20190405_txbsubscribed"


val streamName = "MySecondKinesisStream"

fun getDefaultFullXLSXFilePath(fileName: String) = workingFolder + fileName + fileOutExtension

var totalLines = 0

val ppbumsLogLinesList = mutableListOf<String>()
val upsertedLogLinesList = mutableListOf<String>()
val subscribedLogLinesList = mutableListOf<String>()


val unitToTaxRulesMap = mutableMapOf<String, MutableList<SimpleTaxRule>>()
val unitToUpsertedMap = mutableMapOf<String, MutableList<String>>()
val txBridgeSubscribed: MutableSet<String> = mutableSetOf()


/* TODO: We now use a single stream. Also, tag messages so we can dinstinguish them.
*
* Also, create a sorter / classifier on the Lambda to store messages in appropriate buckets*/
fun main(args: Array<String>) {
    generateKinesisLogs()
//    getYesterday()

}

fun generateKinesisLogs() {


    readPPBUMSplunkLogs()
    readUpsertedSplunkLogs()
    readSuccessfullySubscribedSplunkLogs()

//    var rawppbum = "PPBUM{/units/0001/61ef133f-95df-4b20-9e1f-f80ff5d7e66d|[377852:CITY:Rome:FLAT;377853:CITY:Rome:FLAT;377854:CITY:Rome:FLAT;377855:CITY:Rome:PERCENTAGE]}"
//    decodePPBUM(rawppbum)

//    var rawupserted = "UPSERTED{/units/0000/fd0a80a4-e811-4542-b530-c95c1ccc00c3|[1702036, 1702037, 1702038, 1702039, 1702040, 1702041]}"
//    decodeUPSERTED(rawupserted)

//    var rawsubscribed = "SUBSCRIBED{/units/0001/0e32008f-0402-47fc-aa9b-27bee2de2d7b}"
//    decodeSUBSCRIBED(rawsubscribed)

//    ppbumsLogLinesList.forEach { println(it) }
//    println("SEPARATOR")
//    upsertedLogLinesList.forEach { println(it) }
//    subscribedLogLinesList.forEach { println(it) }
    pushLogsToKinesis()

    //Once we have the log lines. We can push it to kinesis, maybe :hmmm:
}

fun readSuccessfullySubscribedSplunkLogs() {
    val file = File(workingFolder + csvSubscribedInputFileName + fileInExtension)
    val bufferedReader = file.bufferedReader()

    //Skip log headers
    bufferedReader.readLine()

    bufferedReader.forEachLine {
        val tokens = it.split(",")
        if (tokens.isNotEmpty()) {
//            if (tokens.size != 2) {
//                println("tokensSize: " + tokens.size)
//            }
            var unit = cleanFromQuotes(tokens[0])
//            println(unit)

            val logLine = listOf(unit)
                    .joinToString(separator = "|", prefix = "{", postfix = "}")
            ppbumsLogLinesList.add("SUBSCRIBED$logLine" + System.getProperty("line.separator"))
//            println(logLine)
            totalLines++
        }
    }
}

fun readPPBUMSplunkLogs() {
    val file = File(workingFolder + csvPpbumInputFileName + fileInExtension)
    val bufferedReader = file.bufferedReader()

    //Skip log headers
    bufferedReader.readLine()

    bufferedReader.forEachLine {
        val tokens = it.split(",")
        if (tokens.isNotEmpty()) {
//            if (tokens.size != 2) {
//                println("tokensSize: " + tokens.size)
//            }
            var unit = cleanFromQuotes(tokens[0])
//            println(unit)

            var ppbum = cleanFromQuotes(tokens[1])
//            println(ppbum)

            val logLine = listOf(unit, ppbum)
                    .joinToString(separator = "|", prefix = "{", postfix = "}")
            ppbumsLogLinesList.add("PPBUM$logLine" + System.getProperty("line.separator"))
//            println(logLine)
            totalLines++
        }
    }
}

fun readUpsertedSplunkLogs() {
    val file = File(workingFolder + csvUpsertedInputFileName + fileInExtension)
    val bufferedReader = file.bufferedReader()

    //Skip log headers
    bufferedReader.readLine()

    bufferedReader.forEachLine {
        val tokens = it.split(",")
        if (tokens.isNotEmpty()) {
//            if (tokens.size != 2) {
//                println("tokensSize: " + tokens.size)
//            }

            var unit = cleanFromQuotes(tokens[0])
//            println(unit)

            val upserted = cleanFromQuotes(tokens.subList(1, tokens.size).joinToString(separator = ","))

            val logLine = listOf(unit, upserted).joinToString(separator = "|", prefix = "{", postfix = "}")
            upsertedLogLinesList.add("UPSERTED$logLine" + System.getProperty("line.separator"))
//            println(logLine)

            totalLines++

        }
    }
}

fun pushLogsToKinesis() {
    putCollectionToKinesisKPLFutures(streamName, ppbumsLogLinesList)
    putCollectionToKinesisKPLFutures(streamName, upsertedLogLinesList)
    putCollectionToKinesisKPLFutures(streamName, subscribedLogLinesList)
}

//Clean string from quotes (")
fun cleanFromQuotes(raw: String): String =
        if (raw.lastIndexOf("\"") < 0) {
//            println(raw.lastIndexOf(/**/"\"") < 0)
            raw
        } else {
            raw.substring(1 until raw.lastIndexOf("\""))
        }


//this function gets called once for every Proxley log message.
//For this particular example, we are going to call it once per line
//Its kinesis client and stream name, as well as data, are parameterized
private fun putKinesisRecordsWithFailureHandling(data: String, myStreamName: String, amazonKinesisClient: AmazonKinesis) {

    //Setup Kinesis request
    val putRecordsRequest = PutRecordsRequest()
    putRecordsRequest.streamName = myStreamName

    var putRecordsResult: PutRecordsResult

    val putRecordsRequestEntry = PutRecordsRequestEntry()
    putRecordsRequestEntry.data = ByteBuffer.wrap(data.toByteArray())

    var totalPutCalls = 0

    //This goes on Java thread
    do {
        val partitionKey = (Math.random() * 100 + 1).toInt()
        putRecordsRequestEntry.partitionKey = String.format("partitionKey-%d", partitionKey)
        putRecordsRequest.setRecords(listOf(putRecordsRequestEntry))
        putRecordsResult = amazonKinesisClient.putRecords(putRecordsRequest)
        totalPutCalls++
        val putRecordsResultEntry = putRecordsResult.records.get(0)

        if (putRecordsResult.getFailedRecordCount() > 0 && putRecordsResultEntry.getErrorCode() != null) {
            println("Put Result for resubscription after failing$putRecordsResult")
        }

    } while (putRecordsResult.getFailedRecordCount() > 0 && putRecordsResultEntry.getErrorCode() != null)

    println("total put calls for record $data : $totalPutCalls")
}

fun putCollectionToKinesisKPLFutures(streamName: String, listOfData: List<String>) {
    val mapFuturesToData = mutableMapOf<Future<UserRecordResult>, String>()

    //On an ideal architecture, we would have a thread running constantly
    //https://docs.aws.amazon.com/streams/latest/dev/kinesis-kpl-config.html
    val config = KinesisProducerConfiguration()
            .setRecordMaxBufferedTime(3000)
            .setMaxConnections(1)
            .setRequestTimeout(60000)
            //replace region with constant value
            //Or a call to getRegion from AWS Pojo
            .setRegion("us-east-1")

    val kinesisProducer = KinesisProducer(config)

    // Put some records and save the Futures
    val putFutures = LinkedList<Future<UserRecordResult>>()

    //For each data element in collection
    listOfData.forEach { dataElement ->
        val dataByteBuffer = ByteBuffer.wrap(dataElement.toByteArray(StandardCharsets.UTF_8))
        val addUserRecordFuture = kinesisProducer.addUserRecord(streamName, "myPartitionKey", dataByteBuffer)
        // doesn't block
        putFutures.add(addUserRecordFuture)
        mapFuturesToData.put(addUserRecordFuture, dataElement)
    }

    // Wait for puts to finish and check the results
    for (f in putFutures) {
        var result: UserRecordResult? = null // this does block
        try {
            result = f.get()
        } catch (e: InterruptedException) {
            e.printStackTrace()
        } catch (e: ExecutionException) {
            e.printStackTrace()
        }

        if (result!!.isSuccessful()) {
            //This line logs into proxley so we can get a confirmation in as well
            //replace with .log
            println("Put record " + result.toString() + " into shard " +
                    result!!.shardId)
        } else {
            for (attempt in result!!.attempts) {
                // Analyze and respond to the failure
            }
        }
    }
}

//
//private fun proxleyPutCall(data: String, putRecordsRequestEntryList: MutableList<PutRecordsRequestEntry>) {
//
////    for (j in 0..99) {
//    val putRecordsRequestEntry = PutRecordsRequestEntry()
//    putRecordsRequestEntry.data = ByteBuffer.wrap(data.toByteArray())
//    putRecordsRequestEntry.partitionKey = String.format("partitionKey-%d", Math.random())
//    putRecordsRequestEntryList.add(putRecordsRequestEntry)
////    }
//}

fun getYesterday() {

    val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")


    //format date
    var dateFormat = SimpleDateFormat("dd MMM yyyy")

    //variable to store date in string format
    var yesterdayDate: String
    var todayDate: String

    //to get calendar instance
    var cal = Calendar.getInstance()

    println("Today's Day: " + cal.get(Calendar.DAY_OF_MONTH))
    println("Today's Month: " + cal.get(Calendar.MONTH))
    println("Today's Year: " + cal.get(Calendar.YEAR))
    println("Today's Hours: " + cal.get(Calendar.HOUR_OF_DAY))
    println("Today's Minutes: " + cal.get(Calendar.MINUTE))
    println("Today's Seconds: " + cal.get(Calendar.SECOND))
    println("Today's Milliseconds: " + cal.get(Calendar.MILLISECOND))

    todayDate = sdf.format(cal.time);
    println("Today's date = $todayDate")


    //subtract 1 from calendar current date
    cal.add(Calendar.DATE, -1)

    println("Yesterday's Day: " + cal.get(Calendar.DAY_OF_MONTH))
    println("Yesterday's Month: " + cal.get(Calendar.MONTH))
    println("Yesterday's Year: " + cal.get(Calendar.YEAR))
    println("Yesterday's Hours: " + cal.get(Calendar.HOUR_OF_DAY))
    println("Yesterday's Minutes: " + cal.get(Calendar.MINUTE))
    println("Yesterday's Seconds: " + cal.get(Calendar.SECOND))
    println("Yesterday's Milliseconds: " + cal.get(Calendar.MILLISECOND))

    //get formatted date
    yesterdayDate = sdf.format(cal.time)

    println("Yesterday's date = $yesterdayDate")
}


fun decodePPBUM(rawLine: String) {

    //First remove classifier flag
    var unpackedString = rawLine.substring(rawLine.indexOf("{"))
    println("Unpacked: $unpackedString")

    //Remove container
    var noContainer: String = unpackedString.removeSurrounding("{", "}")
    println("NO CONTAINER: $noContainer")

    //Split unit and TaxRule info
    val unitAndRulesTokens = noContainer.split("|")
    unitAndRulesTokens.forEach { println("token $it") }

    val unitUrl = unitAndRulesTokens[0]

    //initialize map with empty list of rules
    if (unitToTaxRulesMap[unitUrl] == null) {
        unitToTaxRulesMap[unitUrl] = mutableListOf()
    }

    val rules = unitAndRulesTokens[1].removeSurrounding("[", "]").split(";")
    processPPBUMS(unitToTaxRulesMap[unitUrl]!!, rules)
}


fun processPPBUMS(rulesForUnitOnMap: MutableList<SimpleTaxRule>, rawRuleList: List<String>) {
    rawRuleList.forEach {
        var ruleData = it.split((":"))
        if (ruleData.size == 4) {
            rulesForUnitOnMap.add(getPPBUM(ruleData))
        }
    }
}

fun getPPBUM(ruleData: List<String>): SimpleTaxRule =
        SimpleTaxRule(
                id = ruleData[0],
                jurisdictionLevel = ruleData[1],
                jurisdictionName = ruleData[2],
                type = ruleData[3])

fun decodeUPSERTED(rawLine: String) {
    println("Raw $rawLine")

    var unpackedString = rawLine.substring(rawLine.indexOf("{"))
    println("Unpacked: $unpackedString")

    //Remove container
    var noContainer: String = unpackedString.removeSurrounding("{", "}")
    println("NO CONTAINER: $noContainer")

    //Split unit and TaxID info
    val unitAndRulesTokens = noContainer.split("|")
    unitAndRulesTokens.forEach { println("token $it") }

    val unitUrl = unitAndRulesTokens[0]

    var ruleIds = unitAndRulesTokens[1].removeSurrounding("[", "]").split(",")


    if (unitToUpsertedMap[unitUrl] == null) {
        unitToUpsertedMap[unitUrl] = mutableListOf()
    }
    ruleIds.forEach { ruleId ->
        unitToUpsertedMap[unitUrl]!!.add(ruleId.replace("\\s".toRegex(), ""))
    }
}

fun decodeSUBSCRIBED(rawLine: String) {
    println(rawLine)

    var unpackedString = rawLine.substring(rawLine.indexOf("{"))
//    println("Unpacked: $unpackedString")

    //Remove container
    var noContainer: String = unpackedString.removeSurrounding("{", "}")
//    println("NO CONTAINER: $noContainer")

    txBridgeSubscribed.add(noContainer)
}

