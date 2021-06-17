package com.acme.kafka.connect.sample

import com.acme.kafka.connect.sample.PropertyUtils.getConnectorVersion
import com.acme.kafka.connect.sample.SampleSourceConnectorConfig.Companion.CONFIG_DEF
import org.apache.kafka.common.config.Config
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigValue
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.connect.util.ConnectorUtils
import org.slf4j.LoggerFactory

class SampleSourceConnector() : SourceConnector() {
    private val log = LoggerFactory.getLogger(SampleSourceConnector::class.java)
    private var originalProps: Map<String, String>? = null
    private var config: SampleSourceConnectorConfig? = null
    private var sourceMonitorThread: SourceMonitorThread? = null
    override fun version(): String {
        return getConnectorVersion()!!
    }

    override fun config(): ConfigDef {
        return CONFIG_DEF
    }

    override fun taskClass(): Class<out Task?> {
        return SampleSourceTask::class.java
    }

    override fun validate(connectorConfigs: Map<String?, String?>): Config {
        val config = super.validate(connectorConfigs)
        val configValues = config.configValues()
        var missingTopicDefinition = true
        for (configValue: ConfigValue in configValues) {
            if (configValue.name() == SampleSourceConnectorConfig.FIRST_REQUIRED_PARAM_CONFIG || configValue.name() == SampleSourceConnectorConfig.SECOND_REQUIRED_PARAM_CONFIG) {
                if (configValue.value() != null) {
                    missingTopicDefinition = false
                    break
                }
            }
        }
        if (missingTopicDefinition) {
            throw ConnectException(
                String.format(
                    "There is no definition of [XYZ] in the "
                            + "configuration. Either the property "
                            + "'%s' or '%s' must be set in the configuration.",
                    SampleSourceConnectorConfig.FIRST_NONREQUIRED_PARAM_CONFIG,
                    SampleSourceConnectorConfig.SECOND_NONREQUIRED_PARAM_CONFIG
                )
            )
        }
        return config
    }

    override fun start(originalProps: Map<String, String>) {
        this.originalProps = originalProps
        config = SampleSourceConnectorConfig(originalProps)
        val firstParam = config!!.getString(SampleSourceConnectorConfig.FIRST_NONREQUIRED_PARAM_CONFIG)
        val secondParam = config!!.getString(SampleSourceConnectorConfig.SECOND_NONREQUIRED_PARAM_CONFIG)
        val monitorThreadTimeout = config!!.getInt(SampleSourceConnectorConfig.MONITOR_THREAD_TIMEOUT_CONFIG)
        sourceMonitorThread = SourceMonitorThread(
            context, firstParam, secondParam, monitorThreadTimeout
        )
        sourceMonitorThread!!.start()
    }

    override fun taskConfigs(maxTasks: Int): List<Map<String?, String?>> {
        var taskConfigs: MutableList<Map<String?, String?>> = mutableListOf()
        // The partitions below represent the source's part that
        // would likely to be broken down into tasks... such as
        // tables in a database.
        val partitions = sourceMonitorThread!!.getCurrentSources()
        if (partitions.isEmpty()) {
            taskConfigs = mutableListOf()
            log.warn("No tasks created because there is zero to work on")
        } else {
            val numTasks = Math.min(partitions.size, maxTasks)
            val partitionSources = ConnectorUtils.groupPartitions(partitions, numTasks)
            for (source: List<String>? in partitionSources) {
                val taskConfig: MutableMap<String?, String?> = HashMap(originalProps)
                taskConfig["sources"] = java.lang.String.join(",", source)
                taskConfigs.add(taskConfig)
            }
        }
        return taskConfigs
    }

    override fun stop() {
        sourceMonitorThread!!.shutdown()
    }
}
