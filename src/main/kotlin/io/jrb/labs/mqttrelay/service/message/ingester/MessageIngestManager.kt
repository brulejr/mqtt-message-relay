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

import io.github.resilience4j.retry.Retry
import io.jrb.labs.common.eventbus.EventBus
import io.jrb.labs.common.logging.LoggerDelegate
import io.jrb.labs.mqttrelay.config.MessageBrokersConfig
import io.jrb.labs.mqttrelay.config.MqttBrokerConfig
import io.jrb.labs.mqttrelay.domain.Message
import io.jrb.labs.mqttrelay.domain.MessageEvent
import io.jrb.labs.mqttrelay.domain.SystemEvent
import io.jrb.labs.mqttrelay.service.message.ingester.mqtt.MqttClientFactory
import io.jrb.labs.mqttrelay.service.message.ingester.mqtt.MqttMessageHandlerImpl
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Service
import reactor.core.Disposable
import java.util.concurrent.atomic.AtomicBoolean

@Service
class MessageIngestManager(
    private val messageBrokersConfig: MessageBrokersConfig,
    private val eventBus: EventBus,
    @Qualifier("retryMqttConnect") private val retry: Retry
) : SmartLifecycle {

    private val log by LoggerDelegate()

    private val _serviceName = javaClass.simpleName
    private val _running: AtomicBoolean = AtomicBoolean()
    private val _messageHandlers: Map<String, MessageHandler>
    private val _messageSubscriptions: MutableMap<String, Disposable> = mutableMapOf()

    private val _scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    init {
        log.info("Initializing {}...", _serviceName)
        _messageHandlers = messageBrokersConfig.mqtt.mapValues { createMqttMessageHandler(it.value) }
    }

    override fun start() {
        _scope.launch {
            log.info("Starting {}...", _serviceName)
            _messageHandlers.forEach {
                val messageHandler: MessageHandler = it.value
                messageHandler.start()
                _messageSubscriptions[it.key] = messageHandler.stream()
                    .doOnEach() { x -> processMessage(it.key, x.get()) }
                    .subscribe()
            }
            eventBus.invokeEvent(SystemEvent("service.start", _serviceName))
            _running.getAndSet(true)
        }
    }

    override fun stop() {
        _scope.launch {
            log.info("Stopping {}...", _serviceName)
            _messageHandlers.forEach {
                it.value.stop()
                _messageSubscriptions[it.key]?.dispose()
            }
            eventBus.invokeEvent(SystemEvent("service.stop", _serviceName))
            _running.getAndSet(true)
        }
    }

    override fun isRunning(): Boolean {
        return _running.get()
    }

    private fun createMqttMessageHandler(brokerConfig: MqttBrokerConfig): MessageHandler {
        log.debug("Creating mqtt message ingester for {}", brokerConfig)
        val connectionFactory = MqttClientFactory(brokerConfig)
        return MqttMessageHandlerImpl(brokerConfig, connectionFactory, retry)
    }

    private fun processMessage(source: String, message: Message?) {
        if (message != null) {
            val filter: Regex? = messageBrokersConfig.mqtt[source]?.injectFilter?.toRegex()
            if ((filter === null) || filter.matches(message.topic)) {
                _scope.launch {
                    eventBus.invokeEvent(MessageEvent(source, "message.in", message!!))
                }
            }
        }
    }

}