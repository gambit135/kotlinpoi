package com.homeaway.pricing.taxes.report

data class UnitData(val unitUrl: String) {

    var city: String = ""
    var county: String = ""
    var country: String = ""
    var district: String = ""
    var localImprovementDistrict: String = ""
    var parish: String = ""
    var specialPurposeDistrict: String = ""
    var state: String = ""
    var transitDistrict: String = ""
    var subscribedTaxRules = mutableListOf<SimpleTaxRule>()
    var notSubscribedTaxRules = listOf<String>()
}