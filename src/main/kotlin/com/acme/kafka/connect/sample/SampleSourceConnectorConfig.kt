package com.acme.kafka.connect.sample

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance

class SampleSourceConnectorConfig(originalProps: Map<*, *>?) :
    AbstractConfig(CONFIG_DEF, originalProps) {
    companion object {
        const val FIRST_REQUIRED_PARAM_CONFIG = "first.required.param"
        private const val FIRST_REQUIRED_PARAM_DOC = "This is the 1st required parameter"
        const val SECOND_REQUIRED_PARAM_CONFIG = "second.required.param"
        private const val SECOND_REQUIRED_PARAM_DOC = "This is the 2nd required parameter"
        const val FIRST_NONREQUIRED_PARAM_CONFIG = "first.nonrequired.param"
        private const val FIRST_NONREQUIRED_PARAM_DOC = "This is the 1st non-required parameter"
        private const val FIRST_NONREQUIRED_PARAM_DEFAULT = "foo"
        const val SECOND_NONREQUIRED_PARAM_CONFIG = "second.nonrequired.param"
        private const val SECOND_NONREQUIRED_PARAM_DOC = "This is the 2ns non-required parameter"
        private const val SECOND_NONREQUIRED_PARAM_DEFAULT = "bar"
        const val MONITOR_THREAD_TIMEOUT_CONFIG = "monitor.thread.timeout"
        private const val MONITOR_THREAD_TIMEOUT_DOC = "Timeout used by the monitoring thread"
        private const val MONITOR_THREAD_TIMEOUT_DEFAULT = 10000

        val CONFIG_DEF get() = createConfigDef()

        private fun createConfigDef(): ConfigDef {
            val configDef = ConfigDef()
            addParams(configDef)
            return configDef
        }

        private fun addParams(configDef: ConfigDef) {
            configDef.define(
                FIRST_REQUIRED_PARAM_CONFIG,
                ConfigDef.Type.STRING,
                Importance.HIGH,
                FIRST_REQUIRED_PARAM_DOC
            )
                .define(
                    SECOND_REQUIRED_PARAM_CONFIG,
                    ConfigDef.Type.STRING,
                    Importance.HIGH,
                    SECOND_REQUIRED_PARAM_DOC
                )
                .define(
                    FIRST_NONREQUIRED_PARAM_CONFIG,
                    ConfigDef.Type.STRING,
                    FIRST_NONREQUIRED_PARAM_DEFAULT,
                    Importance.HIGH,
                    FIRST_NONREQUIRED_PARAM_DOC
                )
                .define(
                    SECOND_NONREQUIRED_PARAM_CONFIG,
                    ConfigDef.Type.STRING,
                    SECOND_NONREQUIRED_PARAM_DEFAULT,
                    Importance.HIGH,
                    SECOND_NONREQUIRED_PARAM_DOC
                )
                .define(
                    MONITOR_THREAD_TIMEOUT_CONFIG,
                    ConfigDef.Type.INT,
                    MONITOR_THREAD_TIMEOUT_DEFAULT,
                    Importance.LOW,
                    MONITOR_THREAD_TIMEOUT_DOC
                )
        }
    }
}