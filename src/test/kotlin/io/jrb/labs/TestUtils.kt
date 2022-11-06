package io.jrb.labs

import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils

interface TestUtils {

    fun randomBoolean() = RandomUtils.nextBoolean()

    fun randomInt() = RandomUtils.nextInt(1, 1000)

    fun randomString() = RandomStringUtils.randomAlphabetic(10)

}