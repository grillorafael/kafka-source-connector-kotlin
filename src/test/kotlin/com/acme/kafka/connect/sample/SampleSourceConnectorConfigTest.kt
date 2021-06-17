package com.acme.kafka.connect.sample

import com.acme.kafka.connect.sample.SampleSourceConnectorConfig
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class SampleSourceConnectorConfigTest {
    @Test
    fun basicParamsAreMandatory() {
        Assertions.assertThrows(
            ConfigException::class.java
        ) {
            val props: Map<String?, String?> =
                HashMap()
            SampleSourceConnectorConfig(props)
        }
    }

    fun checkingNonRequiredDefaults() {
        val props: Map<String?, String?> = HashMap()
        val config = SampleSourceConnectorConfig(props)
        Assertions.assertEquals("foo", config.getString(SampleSourceConnectorConfig.FIRST_NONREQUIRED_PARAM_CONFIG))
        Assertions.assertEquals("bar", config.getString(SampleSourceConnectorConfig.SECOND_NONREQUIRED_PARAM_CONFIG))
    }
}