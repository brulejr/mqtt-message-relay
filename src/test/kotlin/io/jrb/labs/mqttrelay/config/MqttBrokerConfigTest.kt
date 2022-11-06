package io.jrb.labs.mqttrelay.config

import io.jrb.labs.TestUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class MqttBrokerConfigTest : TestUtils {

    var brokerName: String = randomString()
    var broker: String = randomString()
    var qos: Int = randomInt()
    var password: String = randomString()
    var port: Int = randomInt()
    var username: String = randomString()
    var ssl: Boolean = randomBoolean()
    var topic: String = randomString()
    var injectFilter: String = randomString()

    var config: MqttBrokerConfig = MqttBrokerConfig(
        brokerName = brokerName,
        broker = broker,
        qos = qos,
        password = password,
        port = port,
        username = username,
        ssl = ssl,
        topic = topic,
        injectFilter = injectFilter
    )

    @Test
    fun testBean() {
        assertThat(config.brokerName).isEqualTo(brokerName)
        assertThat(config.broker).isEqualTo(broker)
        assertThat(config.qos).isEqualTo(qos)
        assertThat(config.password).isEqualTo(password)
        assertThat(config.port).isEqualTo(port)
        assertThat(config.username).isEqualTo(username)
        assertThat(config.ssl).isEqualTo(ssl)
        assertThat(config.topic).isEqualTo(topic)
        assertThat(config.injectFilter).isEqualTo(injectFilter)
        assertThat(config.tcpUrl).isEqualTo("tcp://" + broker + ":" + port)
    }

    @Test fun testEquals() {
        val config1: MqttBrokerConfig = config.copy()
        assertThat(config1).isEqualTo(config)

        val config2: MqttBrokerConfig = config.copy(
            brokerName = randomString()
        )
        assertThat(config2).isNotEqualTo(config)
    }

    @Test fun testHashcode() {
        val config1: MqttBrokerConfig = config.copy()
        assertThat(config1.hashCode()).isEqualTo(config.hashCode())

        val config2: MqttBrokerConfig = config.copy(
            brokerName = randomString()
        )
        assertThat(config2.hashCode()).isNotEqualTo(config.hashCode())
    }

}