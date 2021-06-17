package com.acme.kafka.connect.sample

import org.apache.kafka.connect.connector.ConnectorContext
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class SourceMonitorThread(context: ConnectorContext?, firstParam: String, secondParam: String, private var monitorThreadTimeout: Int) :  Thread() {

    private val log = LoggerFactory.getLogger(SourceMonitorThread::class.java)
    private val shutdownLatch = CountDownLatch(1)
    private val random = Random(System.currentTimeMillis())

    private var context: ConnectorContext? = context

    override fun run() {
        log.info("Starting thread to monitor topic regex.")
        while (shutdownLatch.count > 0) {
            try {
                // The condition below is just an example of what should be an more elaborated
                // criteria to be done in the source system using the parameters provided.
                if (random.nextInt(monitorThreadTimeout) > monitorThreadTimeout / 2) {
                    log.info("Changes detected in the source. Requesting reconfiguration...")
                    // Here something should be done to update
                    // the list of available sources that the
                    // method 'getCurrentSources()' will return.
                    if (context != null) {
                        context!!.requestTaskReconfiguration()
                    }
                }
                // The hard-coded timeout below of ten seconds should
                // be one of the parameters provided in the connector.
                val shuttingDown = shutdownLatch.await(monitorThreadTimeout.toLong(), TimeUnit.MILLISECONDS)
                if (shuttingDown) {
                    return
                }
            } catch (ie: InterruptedException) {
                log.error("Unexpected InterruptedException, ignoring: ", ie)
            }
        }
    }

    @Synchronized
    fun getCurrentSources(): List<String> {
        return listOf("source-1", "source-2", "source-3")
    }

    fun shutdown() {
        log.info("Shutting down the monitoring thread.")
        shutdownLatch.countDown()
    }
}