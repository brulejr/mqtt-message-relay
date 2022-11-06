package io.jrb.labs.mqttrelay.config

import io.jrb.labs.TestUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class MqttRouterConfigTest : TestUtils {

    var broker: String = randomString()
    var topicFilter: String = randomString()

    var config: MqttRouterConfig = MqttRouterConfig(
        broker = broker,
        topicFilter = topicFilter
    )

    @Test
    fun testBean() {
        assertThat(config.broker).isEqualTo(broker)
        assertThat(config.topicFilter).isEqualTo(topicFilter)
    }

    @Test fun testEquals() {
        val config1: MqttRouterConfig = config.copy()
        assertThat(config1).isEqualTo(config)

        val config2: MqttRouterConfig = config.copy(
            broker = randomString()
        )
        assertThat(config2).isNotEqualTo(config)
    }

    @Test fun testHashcode() {
        val config1: MqttRouterConfig = config.copy()
        assertThat(config1.hashCode()).isEqualTo(config.hashCode())

        val config2: MqttRouterConfig = config.copy(
            broker = randomString()
        )
        assertThat(config2.hashCode()).isNotEqualTo(config.hashCode())
    }

}