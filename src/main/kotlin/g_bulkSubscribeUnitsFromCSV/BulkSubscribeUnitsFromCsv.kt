package g_bulkSubscribeUnitsFromCSV

import com.google.gson.Gson
import kotlinx.coroutines.runBlocking
import okhttp3.MediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import java.io.File
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.math.floor

const val workingFolder = "/Users/atellez/Documents/To-Do/extractUnitMetaData/"
const val fileInExtension = ".csv"
const val csvInputFileName = "unitsMissingFromExpedia"
val readOffset = 2400

const val bulkSubscribeTestUrl = "http://proxley-v2-test.us-east-1-vpc-88394aef.slb-internal.test.aws.away.black/v1/addressEvents/bulkSubscribeUnits"
const val bulkSubscribeStagetUrl = "http://proxley-v2-stage.us-east-1-vpc-35196a52.slb-internal.stage.aws.away.black/v1/addressEvents/bulkSubscribeUnits"
const val bulkSubscribeProdUrl = "http://proxley-v2-production.us-east-1-vpc-d9087bbe.slb-internal.prod.aws.away.black/v1/addressEvents/bulkSubscribeUnits"


//CHANGE THIS WHEN SUBSCRIBING TO PROD or TEST/STAGE
val switchEnvToTryToUOnboardFailedTestOnStage = false
var currentSubscribeEnvUrl = bulkSubscribeProdUrl
val lengthOfUnitUrl = 48
const val batchSize = 600
val unitsFailedToOnboardErrorMessage = "The following units failed to onboard:"
var subscribedSoFar = 0 + readOffset

var failedToOnbardOnCurrentEnv = false
val retryFailedSubscription = false

var totalUnitsToSubscribe = 0
var listOfBatchesOfUnits: LinkedList<UnitBatch> = LinkedList()

var listOfBatchesOfUnitsToSubscribe: LinkedList<UnitBatch> = LinkedList()
var listOfFailedBatches: LinkedList<UnitBatch> = LinkedList()
var listOfFailedBatchesOnCurrentEnv: LinkedList<UnitBatch> = LinkedList()
var listOfSuccessfulBatchesOnProdOrTest: LinkedList<UnitBatch> = LinkedList()
var listOfSuccessfulBatchesOnStage: LinkedList<UnitBatch> = LinkedList()

var currentSuccessfulList = listOfSuccessfulBatchesOnProdOrTest


val JSON = MediaType.parse("application/json; charset=utf-8")
val gson = Gson()


fun main(args: Array<String>) {
    loadUnitsToBulkSubscribe()
    coroutinesApproach()
}

fun coroutinesApproach() = runBlocking {

    println("no. of batches: " + listOfBatchesOfUnits.size)

    listOfBatchesOfUnitsToSubscribe = LinkedList(listOfBatchesOfUnits)
    do {
        println("Progress: $subscribedSoFar of $totalUnitsToSubscribe")
        runBlocking {
            //            listOfFailedBatches = LinkedList()
            listOfFailedBatchesOnCurrentEnv = LinkedList()
            callSuscriptionOnListOfBatches(listOfBatchesOfUnitsToSubscribe)
//            listOfBatchesOfUnitsToSubscribe = listOfFailedBatches
            listOfBatchesOfUnitsToSubscribe = listOfFailedBatchesOnCurrentEnv
        }
        if (failedToOnbardOnCurrentEnv && listOfFailedBatchesOnCurrentEnv.isNotEmpty()) {
            //We start on test, then switch to stage
            //switch to Stage URL
            if (switchEnvToTryToUOnboardFailedTestOnStage && currentSubscribeEnvUrl == bulkSubscribeTestUrl) {
                println("Switching ENV")
                currentSubscribeEnvUrl = bulkSubscribeStagetUrl
                currentSuccessfulList = listOfSuccessfulBatchesOnStage
            } else {
                break
            }
        } else {
            break
        }

    } while (listOfFailedBatchesOnCurrentEnv.isNotEmpty() && retryFailedSubscription)

    //Join successfull batches and see which actually failed on both
    var setOfAllSuccessfulUnits = linkedSetOf<String>()
    listOfSuccessfulBatchesOnProdOrTest
            .map(UnitBatch::listOfUnits)
            .forEach { listOfUnits ->
                listOfUnits.toCollection(setOfAllSuccessfulUnits)
            }
    listOfSuccessfulBatchesOnStage
            .map(UnitBatch::listOfUnits)
            .forEach { listOfUnits ->
                listOfUnits.toCollection(setOfAllSuccessfulUnits)
            }

    //Failed units is dif between All units minus successfull
    var setOfAllUnits = linkedSetOf<String>()
    listOfBatchesOfUnitsToSubscribe
            .flatMap { it.listOfUnits }
            .toCollection(setOfAllUnits)

    var setOfTotalFailedUnits = setOfAllUnits.minus(setOfAllSuccessfulUnits)
    println("No. of FAILED overall units: " + setOfTotalFailedUnits.size)
    println()
    setOfTotalFailedUnits.forEach { print("$it, ") }
    println("No. of FAILED overall units: " + setOfTotalFailedUnits.size)
}



fun callSuscriptionOnListOfBatches(list: LinkedList<UnitBatch>) {
    list.forEachIndexed { index, batch->
        //        filterEmptyAndDefectiveFromBatch(batch)
        var body = buildBody(batch.listOfUnits)

        //RETURN RESPONSE AS A WHOLE OBJECT INSTEAD OF JUST THE HTTP CODE
//        var response = okHttpClientPost(bulkSubscribeProdUrl, body)
        var response = okHttpClientPost(currentSubscribeEnvUrl, body)

        if (response?.responseCode != 204) {

            //Ask here if there are any units that failed to onboard, and add them to ignore list
            if (response?.units?.isNotEmpty()) {

                //Ignore them, really.
                batch.listOfUnits.removeAll(response?.units)

                //Add to failed to resubscribe on another env later
                if (response.units != null) {
                    //listOfFailedBatchesOnCurrentEnv.addLast(createCustomBatchFromListOfUnits(response!!.units))
                    listOfFailedBatchesOnCurrentEnv.add(UnitBatch(LinkedList(response?.units)))
                }

                //The others were successful
                currentSuccessfulList.addLast(batch)
                subscribedSoFar += batch.listOfUnits.size
                println("Subscribed so far: $subscribedSoFar of $totalUnitsToSubscribe, batch $index of ${listOfBatchesOfUnits.size}")

            } else {
                bisectBatch(batch).forEach { half ->
                    listOfFailedBatches.addLast(half)
                }
            }
        } else {
            currentSuccessfulList.addLast(batch)
            subscribedSoFar += batch.listOfUnits.size
            println("Subscribed so far: $subscribedSoFar of $totalUnitsToSubscribe, batch $index of ${listOfBatchesOfUnits.size}")
        }
    }
}

fun createCustomBatchFromListOfUnits(units: List<String>) {
    var currentBatch = UnitBatch()
    var unitsOnPage = 0
    units.forEach { unit ->
        if (unit.isNotBlank()
                && unit.length == lengthOfUnitUrl
                && unit.contains("/units/")) {

            if (unitsOnPage == 0) {
                currentBatch = UnitBatch()
                listOfFailedBatchesOnCurrentEnv.add(currentBatch)
            }

            currentBatch.listOfUnits.add(unit)

            unitsOnPage++
            if (unitsOnPage == batchSize) {
                //Batch is full. Proceed with a new batch for next units
                unitsOnPage = 0
            }
        }
    }
}

/**
 * DEPRECATED. Now the filtering occurs while reading, to avoid storing trash :)
 */
fun filterEmptyAndDefectiveFromBatch(batch: UnitBatch) {
    println("Filtering empty from batch: " + batch.listOfUnits.toString())
    batch.listOfUnits = batch.listOfUnits
            .filter { i -> i.isNotBlank() }
            .toCollection(batch.listOfUnits)
    batch.listOfUnits = batch.listOfUnits
            .filter { i -> i.length == lengthOfUnitUrl && i.contains("/units/") }
            .toCollection(batch.listOfUnits)
}

fun loadUnitsToBulkSubscribe() {
    val file = File(workingFolder + csvInputFileName + fileInExtension)
    val bufferedReader = file.bufferedReader()

    //Skip log headers
    bufferedReader.readLine()

    var totalLines = 0
    var unitsOnPage = 0
    var currentBatch = UnitBatch()
    var localReadOffset = readOffset

    bufferedReader.forEachLine { rawLogLine ->
        if (localReadOffset > 0) {
            localReadOffset--
        } else {
            val tokens = rawLogLine.split(",")
            if (tokens.isNotEmpty()) {
                var unit = tokens[0]
                //remove whitespaces
                unit = unit.replace("\\s".toRegex(), "")

                if (unit.isNotBlank()
                        && unit.length == lengthOfUnitUrl
                        && unit.contains("/units/")) {

                    if (unitsOnPage == 0) {
                        currentBatch = UnitBatch()
                        listOfBatchesOfUnits.add(currentBatch)
                    }

                    currentBatch.listOfUnits.add(unit)

                    unitsOnPage++
                    if (unitsOnPage == batchSize) {
                        //Batch is full. Proceed with a new batch for next units
                        unitsOnPage = 0
                    }
                    totalLines++
                }
            }
        }
    }
    totalUnitsToSubscribe = totalLines
}

fun buildBody(list: LinkedList<String>): String? {
    println("size of batch: " + list.size)
    val jsonifiedBody = gson.toJson(list)
//    println(jsonifiedBody.toString())
    return jsonifiedBody
}

fun okHttpClientPost(url: String, jsonBody: String?): MyCustomResponse {

    var body: RequestBody? = RequestBody.create(JSON, jsonBody)
    val updateRequest = Request.Builder()
            .url(url)
            .post(body)
            .header("Content-Type", "application/json")
            .build()
    var okHttpClient =
            OkHttpClient.Builder()
                    .readTimeout(90, TimeUnit.SECONDS)
                    .connectTimeout(90, TimeUnit.SECONDS)
                    .writeTimeout(90, TimeUnit.SECONDS)
                    .build()
    val response = okHttpClient.newCall(updateRequest).execute()
    val responseCode = response.code()

    if (responseCode == 204) {
//        println("Successful response: " + responseCode + " - " + response.body().toString())
        println("Successful response: $responseCode")
    } else {
//        println("NOT SUCCESSFUL RESPONSE: " + responseCode + " - " + response.body().toString())
        println("NOT SUCCESSFUL RESPONSE: $responseCode")
    }

    val jsonResponseBody = response.body()?.string()
    val responseBodyObject = gson.fromJson(
            jsonResponseBody,
            SubscribeEndpointResponseBody::class.java)

    val responseMessageString = response.message().toString()
    val responseBodyString = responseBodyObject?.toString()

//    println("Message: " + responseMessageString)
//    println("Body: " + responseBodyString)
//
    var units = LinkedList<String>()
    if (responseCode == 500
            && responseMessageString.contains("Internal Server Error")
            && responseBodyString?.contains(unitsFailedToOnboardErrorMessage) ?: false) {
        failedToOnbardOnCurrentEnv = true
        println("units failed to onbard")
        responseBodyObject.message
                .substring(responseBodyObject.message.indexOf("["))
                .removeSurrounding("[", "]")
                .split(",").map { it.trim() }
                .toCollection(units)

//        println("Trying to parse units: " + units.size)
    }

    //Close connection D;
    response?.body()?.close()

    return MyCustomResponse(responseCode, responseMessageString, responseBodyString, units)
}

fun bisectBatch(batch: UnitBatch): MutableList<UnitBatch> {
    var halves: MutableList<UnitBatch> = LinkedList()

    if (batch.listOfUnits.size > 1) {
        var half1 = UnitBatch()
        var half2 = UnitBatch()

        var lowerFirstHalf = 0
        var higherFirstHalf = 0
        var lowerSecondHalf = 0
        var higherSecondHalf = batch.listOfUnits.size - 1

        higherFirstHalf = if (batch.listOfUnits.size % 2 == 0) {
            (batch.listOfUnits.size / 2) - 1
        } else {
            (floor(batch.listOfUnits.size.toDouble() / 2) - 1).toInt()
        }
        lowerSecondHalf = higherFirstHalf + 1

        batch.listOfUnits
                .slice(lowerFirstHalf..higherFirstHalf)
                .toCollection(half1.listOfUnits)

        batch.listOfUnits
                .slice(lowerSecondHalf..higherSecondHalf)
                .toCollection(half2.listOfUnits)

        halves.add(half1)
        halves.add(half2)
    } else {
        halves.add(batch)
    }
    return halves
}
