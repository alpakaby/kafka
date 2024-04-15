package org.alpaka.kafka

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import java.text.SimpleDateFormat
import kotlin.test.Test

internal class EntryTransformTest {
    private val xform: EntryTransform<SourceRecord> = EntryTransform()
    private val schema = SchemaBuilder
        .struct()
        .name("_1SENTRY")
        .version(1)
        .field("ROW_ID", Schema.INT32_SCHEMA)
        .field("DATE_TIME_DOCID", Schema.STRING_SCHEMA)
        .field("SUM_", Schema.FLOAT64_SCHEMA)
        .field("AMOUNT", Schema.FLOAT64_SCHEMA)
        .field("DTSC0", Schema.STRING_SCHEMA)
        .field("DTSC1", Schema.STRING_SCHEMA)
        .field("DTSC2", Schema.STRING_SCHEMA)
        .field("DTSC3", Schema.STRING_SCHEMA)
        .field("KTSC0", Schema.STRING_SCHEMA)
        .field("KTSC1", Schema.STRING_SCHEMA)
        .field("KTSC2", Schema.STRING_SCHEMA)
        .field("KTSC3", Schema.STRING_SCHEMA)
        .field("VDTSC0", Schema.INT32_SCHEMA)
        .field("VDTSC1", Schema.INT32_SCHEMA)
        .field("VKTSC0", Schema.INT32_SCHEMA)
        .field("VKTSC1", Schema.INT32_SCHEMA)
        .build()
    private val formatter = SimpleDateFormat("yyyy-MM-dd")

    @AfterEach
    fun teardown() {
        xform.close()
    }

    @Test
    fun productCreditStruct() {
        val struct = Struct(schema)
            .put("ROW_ID", 20577887)
            .put("DATE_TIME_DOCID", "202202236FQRI8  D1UE   ")
            .put("SUM_", 1330.56)
            .put("AMOUNT", 6048.000)
            .put("DTSC0", "     4       ")
            .put("DTSC1", "             ")
            .put("DTSC2", "             ")
            .put("DTSC3", "             ")
            .put("KTSC0", "   2L9       ")
            .put("KTSC1", "     1       ")
            .put("KTSC2", "             ")
            .put("KTSC3", "             ")
            .put("VDTSC0", 10953)
            .put("VDTSC1", 0)
            .put("VKTSC0", 525)
            .put("VKTSC1", 521)

        val original = SourceRecord(null, null, "test", 0, schema, struct)
        val transformed = xform.apply(original)
        val actualValue = transformed.value() as Struct

        assertEquals(20577887, actualValue.get("id"))
        assertEquals(formatter.parse("2022-02-23"), actualValue.get("date"))
        assertEquals(3357, actualValue.get("source_product"))
        assertEquals(1, actualValue.get("source_warehouse"))
        assertEquals(0, actualValue.get("source_series"))
        assertEquals(null, actualValue.get("target_product"))
        assertEquals(null, actualValue.get("target_warehouse"))
        assertEquals(null, actualValue.get("target_series"))
        assertEquals(6048, actualValue.get("qty"))
        assertEquals(0.22f, actualValue.get("price"))
        assertEquals(false, actualValue.get("reserve"))
    }

    @Test
    fun productCreditReserveStruct() {
        val struct = Struct(schema)
            .put("ROW_ID", 20596674)
            .put("DATE_TIME_DOCID", "202202245UJY34  D25I   ")
            .put("SUM_", 1512.00)
            .put("AMOUNT", 6048.000)
            .put("DTSC0", "             ")
            .put("DTSC1", "             ")
            .put("DTSC2", "             ")
            .put("DTSC3", "             ")
            .put("KTSC0", "   2L9       ")
            .put("KTSC1", "  D1N5       ")
            .put("KTSC2", "     1       ")
            .put("KTSC3", "     1       ")
            .put("VDTSC0", 0)
            .put("VDTSC1", 0)
            .put("VKTSC0", 525)
            .put("VKTSC1", 53074)

        val original = SourceRecord(null, null, "test", 0, schema, struct)
        val transformed = xform.apply(original)
        val actualValue = transformed.value() as Struct

        assertEquals(20596674, actualValue.get("id"))
        assertEquals(formatter.parse("2022-02-24"), actualValue.get("date"))
        assertEquals(3357, actualValue.get("source_product"))
        assertEquals(1, actualValue.get("source_warehouse"))
        assertEquals(1, actualValue.get("source_series"))
        assertEquals(null, actualValue.get("target_product"))
        assertEquals(null, actualValue.get("target_warehouse"))
        assertEquals(null, actualValue.get("target_series"))
        assertEquals(6048, actualValue.get("qty"))
        assertEquals(0.25f, actualValue.get("price"))
        assertEquals(true, actualValue.get("reserve"))
    }

    @Test
    fun productDebitStruct() {
        val struct = Struct(schema)
            .put("ROW_ID", 20609335)
            .put("DATE_TIME_DOCID", "202202215PK4BK  D10S   ")
            .put("SUM_", 6253.51)
            .put("AMOUNT", 30240.000)
            .put("DTSC0", "   2L9       ")
            .put("DTSC1", "     1       ")
            .put("DTSC2", "             ")
            .put("DTSC3", "             ")
            .put("KTSC0", "    NI       ")
            .put("KTSC1", "   EQR       ")
            .put("KTSC2", "             ")
            .put("KTSC3", "             ")
            .put("VDTSC0", 525)
            .put("VDTSC1", 521)
            .put("VKTSC0", 518)
            .put("VKTSC1", 10960)

        val original = SourceRecord(null, null, "test", 0, schema, struct)
        val transformed = xform.apply(original)
        val actualValue = transformed.value() as Struct

        assertEquals(20609335, actualValue.get("id"))
        assertEquals(formatter.parse("2022-02-21"), actualValue.get("date"))
        assertEquals(3357, actualValue.get("target_product"))
        assertEquals(null, actualValue.get("source_product"))
        assertEquals(1, actualValue.get("target_warehouse"))
        assertEquals(null, actualValue.get("source_warehouse"))
        assertEquals(30240, actualValue.get("qty"))
        assertEquals(0.21f, actualValue.get("price"))
        assertEquals(false, actualValue.get("reserve"))
    }

    @Test
    fun productDebitReserveStruct() {
        val struct = Struct(schema)
            .put("ROW_ID", 20669593)
            .put("DATE_TIME_DOCID", "202203017CMYOG  D34S   ")
            .put("SUM_", 1512.00)
            .put("AMOUNT", 6048.000)
            .put("DTSC0", "   2L9       ")
            .put("DTSC1", "  D34S       ")
            .put("DTSC2", "     1       ")
            .put("DTSC3", "     1       ")
            .put("KTSC0", "             ")
            .put("KTSC1", "             ")
            .put("KTSC2", "             ")
            .put("KTSC3", "             ")
            .put("VDTSC0", 525)
            .put("VDTSC1", 53074)
            .put("VKTSC0", 0)
            .put("VKTSC1", 0)

        val original = SourceRecord(null, null, "test", 0, schema, struct)
        val transformed = xform.apply(original)
        val actualValue = transformed.value() as Struct

        assertEquals(20669593, actualValue.get("id"))
        assertEquals(formatter.parse("2022-03-01"), actualValue.get("date"))
        assertEquals(3357, actualValue.get("target_product"))
        assertEquals(null, actualValue.get("source_product"))
        assertEquals(1, actualValue.get("target_warehouse"))
        assertEquals(1, actualValue.get("target_series"))
        assertEquals(null, actualValue.get("source_warehouse"))
        assertEquals(6048, actualValue.get("qty"))
        assertEquals(0.25f, actualValue.get("price"))
        assertEquals(true, actualValue.get("reserve"))
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
