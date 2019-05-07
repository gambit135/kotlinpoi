package g_bulkSubscribeUnitsFromCSV

import java.util.*


class UnitBatch(var listOfUnits: LinkedList<String> = LinkedList()) {


    override fun equals(other: Any?): Boolean {
        if (javaClass != other?.javaClass) return false

        other as UnitBatch
        return other.listOfUnits == listOfUnits
    }

    override fun hashCode(): Int {
        return listOfUnits.hashCode()
    }
}