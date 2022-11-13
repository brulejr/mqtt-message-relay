package io.jrb.labs.mqttrelay.config

import io.jrb.labs.BeanPropertyMap
import io.jrb.labs.TestUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class MqttRouterConfigTest : TestUtils {

    lateinit var configMap: BeanPropertyMap
    lateinit var config: MqttRouterConfig

    @BeforeEach
    fun setup() {
        configMap = createBeanMapForTest(
            klass = MqttRouterConfig::class,
            propsToIgnore = listOf("topicPattern")
        )
        config = createBeanFromMap(MqttRouterConfig::class, configMap)
    }

    @Test
    fun testBean() {
        validateBean(config, configMap)
        assertThat(config.topicPattern?.pattern()).isEqualTo(configMap["topicFilter"])
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