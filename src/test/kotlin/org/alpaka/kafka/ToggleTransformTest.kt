package org.alpaka.kafka

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import java.util.*
import kotlin.test.Test
import kotlin.test.assertNull

internal class ToggleTransformTest {
    private val xformKey: ToggleTransform<SourceRecord> = ToggleTransform.Key()
    private val xformValue: ToggleTransform<SourceRecord> = ToggleTransform.Value()

    @AfterEach
    fun teardown() {
        xformKey.close()
        xformValue.close()
    }

    @Test
    fun handlesNullValue() {
        configure(xformValue)
        val given = SourceRecord(
            null,
            null,
            "topic",
            0,
            null,
            null
        )
        val expected = null
        val actual: Any? = xformValue.apply(given).value()
        assertEquals(expected, actual)
    }

    @Test
    fun handlesNullKey() {
        configure(xformKey)
        val given = SourceRecord(
            null,
            null,
            "topic",
            0,
            null,
            null,
            null,
            null
        )
        val expected = null
        val actual: Any? = xformKey.apply(given).key()
        assertEquals(expected, actual)
    }

    @Test
    fun copyValueSchemaAndConvertFields() {
        configure(xformValue)

        val valueSchema = SchemaBuilder
            .struct()
            .name("name")
            .version(1)
            .doc("doc")
            .field("name", Schema.STRING_SCHEMA)
            .field("is_active", Schema.BOOLEAN_SCHEMA)
            .build()

        val value = Struct(valueSchema)
            .put("name", "Test")
            .put("is_active", false)

        val original = SourceRecord(null, null, "test", 0, valueSchema, value)
        val transformed: SourceRecord = xformValue.apply(original)
        val transformedSchema = transformed.valueSchema()
        val transformedValue = transformed.value() as Struct

        assertEquals(valueSchema.name(), transformedSchema.name())
        assertEquals(valueSchema.version(), transformedSchema.version())
        assertEquals(valueSchema.doc(), transformedSchema.doc())

        assertEquals(Schema.STRING_SCHEMA, transformedSchema.field("name").schema())
        assertEquals("Test", transformedValue.get("name"))

        assertEquals(Schema.BOOLEAN_SCHEMA, transformedSchema.field("is_active").schema())
        assertEquals(true, transformedValue.getBoolean("is_active"))
    }

    @Test
    fun schemalessValueConvertField() {
        configure(xformValue)
        val original = mapOf(
            "name" to "Test",
            "is_active" to false,
        )

        val record = SourceRecord(null, null, "test", 0, null, original)
        val transformed = xformValue.apply(record).value() as Map<*, *>

        assertEquals("Test", transformed["name"])
        assertEquals(true, transformed["is_active"])
    }

    @Test
    fun passUnknownSchemaFields() {
        configure(xformValue)
        val schema = SchemaBuilder
            .struct()
            .name("name")
            .version(1)
            .doc("doc")
            .field("int32", Schema.INT32_SCHEMA)
            .build()

        val expected = Struct(schema).put("int32", 42)
        val original = SourceRecord(null, null, "test", 0, schema, expected)
        val transformed: SourceRecord = xformValue.apply(original)

        assertEquals(schema.name(), transformed.valueSchema().name())
        assertEquals(schema.version(), transformed.valueSchema().version())
        assertEquals(schema.doc(), transformed.valueSchema().doc())
        assertEquals(Schema.INT32_SCHEMA, transformed.valueSchema().field("int32").schema())
        assertEquals(42, (transformed.value() as Struct).getInt32("int32"))
    }

    @Test
    fun topLevelStructRequired() {
        configure(xformValue)
        assertThrows(DataException::class.java) {
            xformValue.apply(
                SourceRecord(
                    null, null,
                    "topic", 0, Schema.INT32_SCHEMA, 42
                )
            )
        }
    }

    @Test
    fun topLevelMapRequired() {
        configure(xformValue)
        assertThrows(DataException::class.java) {
            xformValue.apply(
                SourceRecord(
                    null, null,
                    "topic", 0, null, 42
                )
            )
        }
    }

    @Test
    fun testOptionalStruct() {
        configure(xformValue)
        val builder = SchemaBuilder.struct().optional()
        builder.field("opt_int32", Schema.OPTIONAL_INT32_SCHEMA)
        val schema = builder.build()
        val transformed: SourceRecord = xformValue.apply(
            SourceRecord(
                null, null,
                "topic", 0,
                schema, null
            )
        )
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type())
        assertNull(transformed.value())
    }

    private fun configure(transform: ToggleTransform<SourceRecord>, input: String = "is_active") {
        val props: MutableMap<String, String> = HashMap()

        props["fields"] = input

        transform.configure(props.toMap())
    }
}
