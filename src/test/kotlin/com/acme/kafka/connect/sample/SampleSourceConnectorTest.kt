package com.acme.kafka.connect.sample

import com.acme.kafka.connect.sample.PropertyUtils.getConnectorVersion
import com.acme.kafka.connect.sample.SampleSourceConnector
import com.acme.kafka.connect.sample.SampleSourceTask
import org.apache.kafka.connect.errors.ConnectException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class SampleSourceConnectorTest {
    @Test
    fun connectorVersionShouldMatch() {
        val version = getConnectorVersion()
        Assertions.assertEquals(version, SampleSourceConnector().version())
    }

    @Test
    fun checkClassTask() {
        val taskClass = SampleSourceConnector().taskClass()
        Assertions.assertEquals(SampleSourceTask::class.java, taskClass)
    }

    @Test
    fun checkMissingRequiredParams() {
        Assertions.assertThrows(
            ConnectException::class.java
        ) {
            val props: Map<String?, String?> =
                HashMap()
            SampleSourceConnector().validate(props)
        }
    }
}