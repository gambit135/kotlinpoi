package g_bulkSubscribeUnitsFromCSV

import java.util.*


class UnitBatchOf500(var listOfUnits: LinkedList<String> = LinkedList()) {


    override fun equals(other: Any?): Boolean {
        if (javaClass != other?.javaClass) return false

        other as UnitBatchOf500
        return other.listOfUnits == listOfUnits
    }

    override fun hashCode(): Int {
        return listOfUnits.hashCode()
    }
}