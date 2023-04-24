package org.alpaka.kafka

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import java.util.*
import kotlin.test.Test
import kotlin.test.assertNull

internal class DateTimeIdDocTransformTest {
    private val xformKey: DateTimeIdDocTransform<SourceRecord> = DateTimeIdDocTransform.Key()
    private val xformValue: DateTimeIdDocTransform<SourceRecord> = DateTimeIdDocTransform.Value()

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
        configure(xformValue, "DATE_TIME_IDDOC")

        val schema = SchemaBuilder
            .struct()
            .name("name")
            .version(1)
            .doc("doc")
            .field("DATE_TIME_IDDOC", Schema.STRING_SCHEMA)
            .field("string", Schema.STRING_SCHEMA)
            .build()

        val expected = Struct(schema)
            .put("DATE_TIME_IDDOC", "2023042290WO8W  FGF0   ")
            .put("string", "string")

        val original = SourceRecord(null, null, "test", 0, schema, expected)
        val transformed: SourceRecord = xformValue.apply(original)

        assertEquals(schema.name(), transformed.valueSchema().name())
        assertEquals(schema.version(), transformed.valueSchema().version())
        assertEquals(schema.doc(), transformed.valueSchema().doc())

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema().field("DATE_TIME_IDDOC").schema())
        assertEquals(Date(1682176172000), (transformed.value() as Struct).get("DATE_TIME_IDDOC"))

        assertEquals(Schema.STRING_SCHEMA, transformed.valueSchema().field("string").schema())
        assertEquals("string", (transformed.value() as Struct).getString("string"))
    }

    @Test
    fun schemalessValueConvertField() {
        configure(xformValue, "DATE_TIME_IDDOC")
        val original = mapOf(
            "int32" to 42,
            "DATE_TIME_IDDOC" to "2023042290WO8W  FGF0   "
        )

        val record = SourceRecord(null, null, "test", 0, null, original)
        val transformed = xformValue.apply(record).value() as Map<*, *>

        assertEquals(42, transformed["int32"])
        assertEquals(1682176172000, transformed["DATE_TIME_IDDOC"])
    }

    @Test
    fun passUnknownSchemaFields() {
        configure(xformValue, "DATE_TIME_IDDOC")
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

    private fun configure(transform: DateTimeIdDocTransform<SourceRecord>, field: String = "") {
        val props: MutableMap<String, String> = HashMap()

        props["field"] = field

        transform.configure(props.toMap())
    }
}
