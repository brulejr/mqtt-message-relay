/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2022 Jon Brule <brulejr@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package io.jrb.labs.mqttrelay.service.message.ingester

import io.jrb.labs.common.eventbus.EventBus
import io.jrb.labs.common.logging.LoggerDelegate
import io.jrb.labs.mqttrelay.config.MessageBrokersConfig
import io.jrb.labs.mqttrelay.domain.Message
import io.jrb.labs.mqttrelay.domain.MessageEvent
import io.jrb.labs.mqttrelay.domain.SystemEvent
import io.jrb.labs.mqttrelay.service.message.mqtt.MessageHandlerManager
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Service
import java.util.concurrent.atomic.AtomicBoolean

@Service
class MessageIngestManager(
    private val messageBrokersConfig: MessageBrokersConfig,
    private val messageHandlerManager: MessageHandlerManager,
    private val eventBus: EventBus
) : SmartLifecycle {

    private val log by LoggerDelegate()

    private val _serviceName = javaClass.simpleName
    private val _running: AtomicBoolean = AtomicBoolean()

    override fun start() {
        log.info("Starting {}...", _serviceName)
        messageHandlerManager.subscribe { name, message -> processMessage(name, message) }
        eventBus.sendEvent(SystemEvent("service.start", _serviceName))
        _running.getAndSet(true)
    }

    override fun stop() {
        log.info("Stopping {}...", _serviceName)
        messageHandlerManager.dispose()
        eventBus.sendEvent(SystemEvent("service.stop", _serviceName))
        _running.getAndSet(true)
    }

    override fun isRunning(): Boolean {
        return _running.get()
    }

    private fun processMessage(source: String, message: Message?) {
        if (message != null) {
            val filter: Regex? = messageBrokersConfig.mqtt[source]?.injectFilter?.toRegex()
            if ((filter === null) || filter.matches(message.topic)) {
                eventBus.sendEvent(MessageEvent(source, "message.in", message))
            }
        }
    }

}