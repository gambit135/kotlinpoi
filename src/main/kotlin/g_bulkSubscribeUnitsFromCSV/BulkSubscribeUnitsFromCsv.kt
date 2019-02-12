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
const val csvInputFileName = "Unit URLs to Resubscribe 02.08.19"


const val batchSize = 500

var subscribedSoFar = 0
var totalUnitsToSubscribe = 0

var listOfBatchesOfUnits: LinkedList<UnitBatchOf500> = LinkedList()
var listOfBatchesOfUnitsToSubscribe: LinkedList<UnitBatchOf500> = LinkedList()
var listOfFailedBatches: LinkedList<UnitBatchOf500> = LinkedList()
var listOfSuccessfulBatches: LinkedList<UnitBatchOf500> = LinkedList()


val JSON = MediaType.parse("application/json; charset=utf-8")

const val bulkSubscribeProdUrl = "http://proxley-v2-production.us-east-1-vpc-d9087bbe.slb-internal.prod.aws.away.black/v1/addressEvents/bulkSubscribeUnits"
const val bulkSubscribeTestUrl = "http://proxley-v2-test.us-east-1-vpc-88394aef.slb-internal.test.aws.away.black/v1/addressEvents/bulkSubscribeUnits"

fun main(args: Array<String>) {
    coroutinesApproach()
}

fun coroutinesApproach() = runBlocking {
    loadUnitsToBulkSubscribe()

    println("no. of batches: " + listOfBatchesOfUnits.size)

    listOfBatchesOfUnitsToSubscribe = LinkedList(listOfBatchesOfUnits)
    do {
        println("Progress: $subscribedSoFar of $totalUnitsToSubscribe")
        runBlocking {
            listOfFailedBatches = LinkedList()
            callSuscriptionOnListOfBatches(listOfBatchesOfUnitsToSubscribe)
            listOfBatchesOfUnitsToSubscribe = listOfFailedBatches
        }

    } while (!listOfFailedBatches.isEmpty())
}

fun callSuscriptionOnListOfBatches(list: LinkedList<UnitBatchOf500>) {
    list.forEach { batch ->
        filterEmptyFromBatch(batch)
        var body = buildBody(batch.arrayOfUnits)
        var status = okHttpClientPost(bulkSubscribeProdUrl, body)
        if (status != 204) {
            bisectBatch(batch).forEach { half ->
                listOfFailedBatches.addLast(half)
            }
        } else {
            listOfSuccessfulBatches.addLast(batch)
            subscribedSoFar += batch.arrayOfUnits.size
            println("Subscribed so far: $subscribedSoFar of $totalUnitsToSubscribe")
        }
    }
}

fun filterEmptyFromBatch(batch: UnitBatchOf500) {
    println("Filtering empty from batch: " + batch.arrayOfUnits.toString())
    batch.arrayOfUnits = batch.arrayOfUnits.filter { i -> i.isNotBlank() }.toTypedArray()
}

fun loadUnitsToBulkSubscribe() {
    val file = File(workingFolder + csvInputFileName + fileInExtension)
    val bufferedReader = file.bufferedReader()

    //Skip log headers
    bufferedReader.readLine()

    var totalLines = 0
    var unitsOnPage = 0
    var currentBatch: UnitBatchOf500 = UnitBatchOf500()
    bufferedReader.forEachLine {
        val tokens = it.split(",")
        if (tokens.isNotEmpty()) {

            if (unitsOnPage == 0) {
                currentBatch = UnitBatchOf500()
                listOfBatchesOfUnits.add(currentBatch)
            }

            var unit = tokens[0]
            //remove whitespaces
            unit =  unit.replace("\\s".toRegex(), "")
            currentBatch.arrayOfUnits[unitsOnPage] = unit
            unitsOnPage++

            if (unitsOnPage == batchSize) {
                //Batch is full. Proceed with a new batch for next units
                unitsOnPage = 0
            }
            totalLines++
        }
    }
    totalUnitsToSubscribe = totalLines
}

fun buildBody(array: Array<String>): String? {
    println("size of batch: " + array.size)
    val gson = Gson()
    val jsonifiedBody = gson.toJson(array)
    println(jsonifiedBody.toString())
    return jsonifiedBody
}

fun okHttpClientPost(url: String, jsonBody: String?): Int {

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
    if (response.code() == 204) {
        println("Successful response: " + response.code() + " - " + response.body().toString())
    } else {
        println("NOT SUCCESSFUL RESPONSE: " + response.code() + " - " + response.body().toString())
    }
    return response.code()
}

fun bisectBatch(batch: UnitBatchOf500): MutableList<UnitBatchOf500> {
    var halves: MutableList<UnitBatchOf500> = LinkedList()

    if (batch.arrayOfUnits.size > 1) {
        var half1 = UnitBatchOf500()
        var half2 = UnitBatchOf500()

        var lowerFirstHalf = 0
        var higherFirstHalf = 0
        var lowerSecondHalf = 0
        var higherSecondHalf = batch.arrayOfUnits.size - 1

        higherFirstHalf = if (batch.arrayOfUnits.size % 2 == 0) {
            (batch.arrayOfUnits.size / 2) - 1
        } else {
            (floor(batch.arrayOfUnits.size.toDouble() / 2) - 1).toInt()
        }
        lowerSecondHalf = higherFirstHalf + 1

        half1.arrayOfUnits = batch.arrayOfUnits.sliceArray(lowerFirstHalf..higherFirstHalf)
        half2.arrayOfUnits = batch.arrayOfUnits.sliceArray(lowerSecondHalf..higherSecondHalf)

        halves.add(half1)
        halves.add(half2)
    } else {
        halves.add(batch)
    }
    return halves
}
