package c_findUnitsWithSameRequestMarker

data class UnitMetaData (
        var requestMarkerOccurences: HashMap<String, Byte>? = null,
        var metaListOfTaxRuleJurisdictions: MutableList<MutableList<Jurisdiction>>? = null,
        var unitUrl:String = "",
        var hasEmptyRequests:Boolean = false,
        var country:String = "",
        var state:String = "",
        var district:String = "",
        var county:String = "",
        var city:String = ""
)