package com.acme.kafka.connect.sample

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

object PropertyUtils {
    private val CONNECTOR_VERSION = "connector.version"

    private val log: Logger = LoggerFactory.getLogger(PropertyUtils::class.java)
    private val propertiesFile = "/kafka-connect-sample.properties"
    private var properties: Properties? = null

    init {
        try {
            val stream = PropertyUtils::class.java.getResourceAsStream(propertiesFile)
            properties = Properties()
            properties!!.load(stream)
        } catch (ex: Exception) {
            log.warn("Error while loading properties: ", ex);
        }
    }

    @JvmStatic
    fun getConnectorVersion(): String? {
        return properties!!.getProperty(CONNECTOR_VERSION)
    }
}
