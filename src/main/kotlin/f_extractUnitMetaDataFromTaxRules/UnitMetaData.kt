package f_extractUnitMetaDataFromTaxRules

data class UnitMetaData (
        var metaListOfTaxRuleJurisdictions: MutableList<MutableList<Jurisdiction>>? = null,
        var unitUrl:String = "",
        var country:String = "",
        var state:String = "",
        var district:String = "",
        var county:String = "",
        var city:String = "",
        var malformedUrl:Boolean = false
)