package org.alpaka.kafka

import io.debezium.spi.converter.CustomConverter.Converter
import io.debezium.spi.converter.CustomConverter.ConverterRegistration
import io.debezium.spi.converter.RelationalColumn
import org.apache.kafka.connect.data.SchemaBuilder
import java.util.*
import kotlin.test.Test
import kotlin.test.assertEquals


internal class UuidConverterTest {
    private class MockRegistration<S> : ConverterRegistration<S> {
        var converter: Converter? = null
        var schema: S? = null

        override fun register(fieldSchema: S, converter: Converter?) {
            this.converter = converter
            this.schema = fieldSchema
        }
    }

    @Test
    fun convertBytesToUuid() {
        val column = getColumn("_idrref");
        val registration = MockRegistration<SchemaBuilder>()
        val converter = UuidConverter()

        converter.configure(Properties())
        converter.converterFor(column, registration)

        val input = "hW98wlVlFywR7v8T1kGk8A=="
        val result = registration.converter?.convert(input)

        assertEquals("856f7cc2-5565-172c-11ee-ff13d641a4f0", result)
    }

    private fun getColumn(name: String): RelationalColumn {
        return object : RelationalColumn {
            override fun typeName(): String {
                return "bytea"
            }

            override fun name(): String {
                return name
            }

            override fun dataCollection(): String? {
                return null
            }

            override fun typeExpression(): String? {
                return null
            }

            override fun scale(): OptionalInt? {
                return null
            }

            override fun nativeType(): Int {
                return 0
            }

            override fun length(): OptionalInt? {
                return null
            }

            override fun jdbcType(): Int {
                return 0
            }

            override fun isOptional(): Boolean {
                return false
            }

            override fun hasDefaultValue(): Boolean {
                return false
            }

            override fun defaultValue(): Any? {
                return null
            }
        }
    }
}
