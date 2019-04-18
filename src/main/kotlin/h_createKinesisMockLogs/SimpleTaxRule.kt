package com.homeaway.pricing.taxes.report

data class SimpleTaxRule(
        val id: String,
        val jurisdictionLevel: String,
        val jurisdictionName: String,
        val type: String) {

    override fun toString() = "$id:$jurisdictionLevel:$jurisdictionName:$type"
    override fun equals(other: Any?): Boolean {
        return when (other) {
            is SimpleTaxRule -> other.id == this.id
            else -> false
        }
    }
}