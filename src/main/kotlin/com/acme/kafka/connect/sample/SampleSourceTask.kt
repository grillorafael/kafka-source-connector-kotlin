package com.acme.kafka.connect.sample

import com.acme.kafka.connect.sample.PropertyUtils.getConnectorVersion
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.slf4j.LoggerFactory
import java.util.*

class SampleSourceTask : SourceTask() {
    private val log = LoggerFactory.getLogger(SampleSourceTask::class.java)

    private var monitorThreadTimeout = 0
    private var sources: List<String>? = null

    override fun version(): String? {
        return getConnectorVersion()
    }

    override fun start(properties: Map<String?, String?>) {
        val config = SampleSourceConnectorConfig(properties)
        monitorThreadTimeout = config.getInt(SampleSourceConnectorConfig.MONITOR_THREAD_TIMEOUT_CONFIG)
        val sourcesStr = properties["sources"]
        sources = Arrays.asList(*sourcesStr!!.split(",").toTypedArray())
    }

    @Throws(InterruptedException::class)
    override fun poll(): List<SourceRecord>? {
        Thread.sleep((monitorThreadTimeout / 2).toLong())
        val records: MutableList<SourceRecord> = ArrayList()
        for (source in sources!!) {
            log.info("Polling data from the source '$source'")
            records.add(
                SourceRecord(
                    Collections.singletonMap("source", source),
                    Collections.singletonMap("offset", 0),
                    source, null, null, null, Schema.BYTES_SCHEMA,
                    String.format("Data from %s", source).toByteArray()
                )
            )
        }
        return records
    }

    override fun stop() {}
}