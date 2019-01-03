package g_bulkSubscribeUnitsFromCSV

data class UnitBatchOf500 (var arrayOfUnits:Array<String> = Array<String>(500){""}) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as UnitBatchOf500

        if (!arrayOfUnits.contentEquals(other.arrayOfUnits)) return false

        return true
    }

    override fun hashCode(): Int {
        return arrayOfUnits.contentHashCode()
    }
}