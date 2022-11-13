package io.jrb.labs.mqttrelay.config

import io.jrb.labs.BeanPropertyMap
import io.jrb.labs.TestUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class MqttBrokerConfigTest : TestUtils {

    lateinit var configMap: BeanPropertyMap
    lateinit var config: MqttBrokerConfig

    @BeforeEach
    fun setup() {
        configMap = createBeanMapForTest(
            klass = MqttBrokerConfig::class,
            propsToIgnore = listOf("tcpUrl")
        )
        config = createBeanFromMap(MqttBrokerConfig::class, configMap)
    }

    @Test
    fun testBean() {
        validateBean(config, configMap)
        assertThat(config.tcpUrl).isEqualTo("tcp://" + configMap["broker"] + ":" + configMap["port"])
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