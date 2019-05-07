package i_compareActiveUnitsExpediaHA

import java.io.File
import java.util.*


val myWorkingFolder = "/Users/atellez/Documents/To-Do/compareActiveUnits/"
val myHAcsvInputFileName = "homeaway_active_units"
val myEXPEcsvInputFileName = "expe_HA_list 2"
val outputFileName = "unitsMissingFromExpedia"
val myCsvFileExtension = ".csv"

val myLengthOfUnitUrl = 48

var expediaUnits = TreeSet<String>()
var homeawayUnits = TreeSet<String>()

val addFun: (TreeSet<String>, String) -> Unit = { set, s ->
    if (s.length == myLengthOfUnitUrl) {
        set.add(s)
    }
}
val printFun: (TreeSet<String>, String) -> Unit = { set, s -> println(s) }

val addAnonFun = fun(set: TreeSet<String>, s: String): Unit {
    if (s.length == myLengthOfUnitUrl) {
        set.add(s)
    }
}

fun main(args: Array<String>) {
    //read EXPE
    iterateLinesInFile(expediaUnits, myWorkingFolder + myEXPEcsvInputFileName + myCsvFileExtension, addFun)

    //read HA
    iterateLinesInFile(homeawayUnits, myWorkingFolder + myHAcsvInputFileName + myCsvFileExtension, addFun)

    var difference : TreeSet<String> = sortedSetOf()
    homeawayUnits.minus(expediaUnits).toCollection(difference)


    writeCSVFile(difference, myWorkingFolder + outputFileName + myCsvFileExtension)
//    iterateLinesInFile(difference as TreeSet<String>, myWorkingFolder + outputFileName + myCsvFileExtension, printFun)
    println("No. of units from HA not on Expedia is: " + difference.size)

    println("No. of EXPE units not on HA is: " + expediaUnits.minus(homeawayUnits).size)
}

fun writeCSVFile(mySet: Set<String>, fileName: String) {
    val myFile = File(fileName)
    myFile.bufferedWriter().use { out ->
        out.write("This is a unit header,\n")
        mySet.forEach { line ->
            out.write("$line,\n")
        }
    }
    println("Written to file")
}

fun iterateLinesInFile(currentSet: TreeSet<String>, fileName: String, code: (TreeSet<String>, String) -> Unit) {
    val file = File(fileName)
    val bufferedReader = file.bufferedReader()
    bufferedReader.forEachLine { rawLogLine ->
        code(currentSet, rawLogLine)
    }
}

