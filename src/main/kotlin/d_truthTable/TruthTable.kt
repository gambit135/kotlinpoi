package d_truthTable

import kotlin.math.max


fun main(args: Array<String>) {
    println(message = "Hello, Kotlin!")
    truthTable()
}

fun truthTable() {
    var lodgingTaxesOnOFPPropertiesIsEnabled = listOf(false, true)
    var getOlbIntegrated = listOf(false, true)
    var getOnlinePayAdopted = listOf(false, true)
    var getOfflinePayAdopted = listOf(false, true)
    var getOfflinePayDeprecated = listOf(false, true)

    println("lodgingTaxesOnOFPPropertiesIsEnabledElement \t " +
            "olbIntegratedElement \t" +
            "onlinePayAdoptedElement \t" +
            "offlinePayAdoptedElement \t" +
            "offlinePayDeprecatedElement \t" +
            "TRUTH")

    lodgingTaxesOnOFPPropertiesIsEnabled.forEach { lodgingTaxesOnOFPPropertiesIsEnabledElement ->
        getOlbIntegrated.forEach { getOlbIntegratedElement ->
            getOnlinePayAdopted.forEach { getOnlinePayAdoptedElement ->
                getOfflinePayAdopted.forEach { getOfflinePayAdoptedElement ->
                    getOfflinePayDeprecated.forEach { getOfflinePayDeprecatedElement ->
                        printElements(
                                lodgingTaxesOnOFPPropertiesIsEnabledElement,
                                getOlbIntegratedElement,
                                getOnlinePayAdoptedElement,
                                getOfflinePayAdoptedElement,
                                getOfflinePayDeprecatedElement
                        )
                    }
                }

            }
        }
    }

}

fun printElements(
        lodgingTaxesOnOFPPropertiesIsEnabledElement: Boolean,
        olbIntegratedElement: Boolean,
        onlinePayAdoptedElement: Boolean,
        offlinePayAdoptedElement: Boolean,
        offlinePayDeprecatedElement: Boolean
) {
    println("$lodgingTaxesOnOFPPropertiesIsEnabledElement \t\t\t\t\t\t\t\t\t\t\t\t" +
            "$olbIntegratedElement \t\t\t\t\t\t" +
            "$onlinePayAdoptedElement \t\t\t\t\t" +
            "$offlinePayAdoptedElement \t\t\t\t\t\t" +
            "$offlinePayDeprecatedElement\t\t\t\t\t\t" +
            calculateTruth(lodgingTaxesOnOFPPropertiesIsEnabledElement,
                    olbIntegratedElement,
                    onlinePayAdoptedElement,
                    offlinePayAdoptedElement,
                    offlinePayDeprecatedElement))

}

fun calculateTruth(
        lodgingTaxesOnOFPPropertiesIsEnabledElement: Boolean,
        olbIntegratedElement: Boolean,
        onlinePayAdoptedElement: Boolean,
        offlinePayAdoptedElement: Boolean,
        offlinePayDeprecatedElement: Boolean )=
        shouldEnableUnitLodgingTax(
                lodgingTaxesOnOFPPropertiesIsEnabledElement,
                olbIntegratedElement,
                onlinePayAdoptedElement,
                offlinePayAdoptedElement,
                offlinePayDeprecatedElement
        )

fun shouldEnableUnitLodgingTax(lodgingTaxesOnOFPPropertiesIsEnabledElement: Boolean,
                               olbIntegratedElement: Boolean,
                               onlinePayAdoptedElement: Boolean,
                               offlinePayAdoptedElement: Boolean,
                               offlinePayDeprecatedElement: Boolean) =
        if (lodgingTaxesOnOFPPropertiesIsEnabledElement)
            !olbIntegratedElement
        else
            checkOfflinePaymentFlags(offlinePayAdoptedElement, offlinePayDeprecatedElement) && !olbIntegratedElement && onlinePayAdoptedElement


private fun checkOfflinePaymentFlags(offlinePayAdoptedElement: Boolean, offlinePayDeprecatedElement: Boolean) =
        !offlinePayAdoptedElement || (offlinePayAdoptedElement && offlinePayDeprecatedElement)

