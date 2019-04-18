package g_bulkSubscribeUnitsFromCSV

import java.util.LinkedList


data class UnitBatchOf500 (var listOfUnits: LinkedList<String> = LinkedList()) {
    override fun equals(other: Any?): Boolean {
//        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as UnitBatchOf500
        return other.listOfUnits == listOfUnits
//        if (listOfUnits.containsAll(other.listOfUnits)
//        && other.listOfUnits.containsAll(listOfUnits)) return true
//
//        return false
    }

    override fun hashCode(): Int {
        return listOfUnits.hashCode()
    }
}