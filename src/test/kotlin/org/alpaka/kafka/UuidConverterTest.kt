package org.alpaka.kafka

import io.debezium.spi.converter.CustomConverter.Converter
import io.debezium.spi.converter.CustomConverter.ConverterRegistration
import io.debezium.spi.converter.RelationalColumn
import org.apache.kafka.connect.data.SchemaBuilder
import java.nio.ByteBuffer
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
        val column = getColumn("test");
        val registration = MockRegistration<SchemaBuilder>()
        val converter = UuidConverter()
        val properties = Properties()

        properties.setProperty("columns", "public.table.test")

        converter.configure(properties)
        converter.converterFor(column, registration)

        val input = UUID.fromString("856f7cc2-5565-172c-11ef-3ce4edeaf984")
        val result = registration.converter?.convert(uuidToBytes(input))

        assertEquals("edeaf984-3ce4-11ef-856f-7cc25565172c", result)
    }

    private fun getColumn(name: String): RelationalColumn {
        return object : RelationalColumn {
            override fun typeName(): String {
                return "bytea"
            }

            override fun name(): String {
                return name
            }

            override fun dataCollection(): String {
                return "public.table"
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

    private  fun uuidToBytes(uuid: UUID): ByteArray {
        val byteBuffer = ByteBuffer.allocate(16)
        byteBuffer.putLong(uuid.mostSignificantBits)
        byteBuffer.putLong(uuid.leastSignificantBits)
        return byteBuffer.array()
    }
}
