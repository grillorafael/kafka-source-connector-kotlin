package com.acme.kafka.connect.sample

import com.acme.kafka.connect.sample.PropertyUtils.getConnectorVersion
import com.acme.kafka.connect.sample.SampleSourceConnector
import com.acme.kafka.connect.sample.SampleSourceConnectorConfig
import com.acme.kafka.connect.sample.SampleSourceTask
import org.junit.Test
import org.junit.jupiter.api.Assertions

class SampleSourceTaskTest {
    @Test
    fun taskVersionShouldMatch() {
        val version = getConnectorVersion()
        Assertions.assertEquals(version, SampleSourceTask().version())
    }

    @Test
    fun checkNumberOfRecords() {
        val connectorProps: MutableMap<String, String> = HashMap()
        connectorProps[SampleSourceConnectorConfig.FIRST_REQUIRED_PARAM_CONFIG] = "Kafka"
        connectorProps[SampleSourceConnectorConfig.SECOND_REQUIRED_PARAM_CONFIG] = "Connect"
        val taskProps = getTaskProps(connectorProps)
        val task = SampleSourceTask()
        Assertions.assertDoesNotThrow {
            task.start(taskProps)
            val records = task.poll()
            Assertions.assertEquals(3, records!!.size)
        }
    }

    private fun getTaskProps(connectorProps: Map<String, String>): Map<String?, String?> {
        val connector = SampleSourceConnector()
        connector.start(connectorProps)
        val taskConfigs: List<Map<String?, String?>> = connector.taskConfigs(1)
        return taskConfigs[0]
    }
}