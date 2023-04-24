package org.alpaka.kafka

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import kotlin.test.Test

internal class RelationTransformTest {
    private val xform: RelationTransform<SourceRecord> = RelationTransform()
    private val schema = SchemaBuilder
        .struct()
        .name("_1SCRDOC")
        .version(1)
        .field("ROW_ID", Schema.INT32_SCHEMA)
        .field("MDID", Schema.INT32_SCHEMA)
        .field("PARENTVAL", Schema.STRING_SCHEMA)
        .field("CHILDID", Schema.STRING_SCHEMA)
        .build()

    @AfterEach
    fun teardown() {
        xform.close()
    }

    @Test
    fun processStruct() {
        val struct = Struct(schema)
            .put("ROW_ID", 2174067)
            .put("MDID", 0)
            .put("PARENTVAL", "O1 9NP  FGBI           ")
            .put("CHILDID", "  FGLF   ")

        val original = SourceRecord(null, null, "test", 0, schema, struct)
        val actual = xform.apply(original).value() as Struct

        assertEquals(2174067, actual.get("id"))
        assertEquals(721347, actual.get("child"))
        assertEquals(720990, actual.get("parent"))
        assertEquals(0, actual.get("section"))
    }

    @Test
    fun processIncompleteParent() {
        val struct = Struct(schema)
            .put("ROW_ID", 2174067)
            .put("MDID", 0)
            .put("PARENTVAL", "U                      ")
            .put("CHILDID", "  FGLF   ")

        val original = SourceRecord(null, null, "test", 0, schema, struct)
        val actual = xform.apply(original).value() as Struct

        assertEquals(2174067, actual.get("id"))
        assertEquals(721347, actual.get("child"))
        assertEquals(0, actual.get("parent"))
        assertEquals(0, actual.get("section"))
    }

    @Test
    fun topLevelStructRequired() {
        assertThrows(DataException::class.java) {
            xform.apply(
                SourceRecord(
                    null, null,
                    "topic", 0, Schema.INT32_SCHEMA, 42
                )
            )
        }
    }
}
