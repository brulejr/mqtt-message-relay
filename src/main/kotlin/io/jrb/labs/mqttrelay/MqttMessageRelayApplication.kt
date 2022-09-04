package io.jrb.labs.mqttrelay

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class MqttMessageRelayApplication

fun main(args: Array<String>) {
	runApplication<MqttMessageRelayApplication>(*args)
}
