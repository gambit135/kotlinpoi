package g_bulkSubscribeUnitsFromCSV

data class MyCustomResponse(
        var responseCode: Int,
        var responseMessageString: String,
        var responseBodyString: String?,
        var units: List<String>)