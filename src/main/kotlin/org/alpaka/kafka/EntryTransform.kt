package org.alpaka.kafka

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import java.math.RoundingMode
import java.text.SimpleDateFormat
import java.util.*
import kotlin.math.absoluteValue

class EntryTransform<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        const val OVERVIEW_DOC = "Transforms internal 1C 7.7 entries information to leftover table format"

        val CONFIG_DEF: ConfigDef = ConfigDef()

        private const val PURPOSE = "entry-convert"
        private const val DATE_PART_START = 0
        private const val DATE_PART_END = 8
        private const val MARK_PRODUCT = 525
        private const val MARK_RESERVE = 53074
        private const val INT_RADIX = 36

        private val keySchema = SchemaBuilder.struct()
            .name("entry-value")
            .doc("Entry value")
            .version(1)
            .field("id", Schema.INT32_SCHEMA)

        private val valueSchema = SchemaBuilder.struct()
            .name("entry-value")
            .doc("Entry value")
            .version(1)
            .field("id", Schema.INT32_SCHEMA)
            .field("date", Date.SCHEMA)
            .field("source_product", Schema.OPTIONAL_INT32_SCHEMA)
            .field("source_warehouse", Schema.OPTIONAL_INT32_SCHEMA)
            .field("source_series", Schema.OPTIONAL_INT32_SCHEMA)
            .field("target_product", Schema.OPTIONAL_INT32_SCHEMA)
            .field("target_warehouse", Schema.OPTIONAL_INT32_SCHEMA)
            .field("target_series", Schema.OPTIONAL_INT32_SCHEMA)
            .field("qty", Schema.INT32_SCHEMA)
            .field("price", Schema.FLOAT32_SCHEMA)
            .field("reserve", Schema.BOOLEAN_SCHEMA)
            .build()
    }

    private val inputFormatter = SimpleDateFormat("yyyyMMdd")

    override fun configure(props: Map<String?, *>?) {
        inputFormatter.timeZone = TimeZone.getTimeZone("UTC")
    }

    override fun apply(record: R): R = when {
        record?.value() == null -> {
            applyKeyOnly(record)
        }

        else -> {
            applyWithSchema(record)
        }
    }

    @Suppress("EmptyFunctionBlock")
    override fun close() {
    }

    override fun config(): ConfigDef = CONFIG_DEF

    private fun applyKeyOnly(record: R): R {
        val value = Requirements.requireStruct(record?.key(), PURPOSE)
        val key = Struct(keySchema).put("id", value.getInt32("ROW_ID"))

        return record!!.newRecord(
            record.topic(),
            record.kafkaPartition(),
            key.schema(),
            key,
            null,
            null,
            record.timestamp()
        )
    }

    private fun applyWithSchema(record: R): R {
        val value = convert(Requirements.requireStruct(record?.value(), PURPOSE))
        val key = Struct(keySchema).put("id", value.getInt32("id"))

        return record!!.newRecord(
            record.topic(),
            record.kafkaPartition(),
            key.schema(),
            key,
            value.schema(),
            value,
            record.timestamp()
        )
    }

    private fun convert(value: Struct): Struct {
        val id = value.getInt32("ROW_ID")
        val date = inputFormatter.parse(value.getString("DATE_TIME_DOCID").substring(DATE_PART_START, DATE_PART_END))
        val qty = value.getFloat64("AMOUNT").toInt()
        val price = when (qty) {
            0 -> 0.0
            else -> value.getFloat64("SUM_").absoluteValue / qty.absoluteValue
        }

        val result = Struct(valueSchema)
            .put("id", id)
            .put("date", date)
            .put("qty", qty)
            .put("price", price.round(2).toFloat())
            .put("reserve", false)

        fillTarget(value, result)
        fillSource(value, result)

        return result
    }

    private fun fillTarget(value: Struct, result: Struct)
    {
        if (value.getInt32("VDTSC0") != MARK_PRODUCT) {
            return
        }

        val reserve = value.getInt32("VDTSC1") == MARK_RESERVE

        val warehouse = when {
            reserve -> "DTSC2"
            else -> "DTSC1"
        }

        val series = when {
            reserve -> "DTSC3"
            else -> "DTSC2"
        }

        val seriesValue = value.getString(series).trim()

        val targetProduct = value.getString("DTSC0").trim().toInt(INT_RADIX)
        val targetWarehouse = value.getString(warehouse).trim().toInt(INT_RADIX)
        val targetSeries = if (seriesValue.isNotEmpty()) {
            seriesValue.toInt(INT_RADIX)
        } else {
            0
        }

        result
            .put("target_product", targetProduct)
            .put("target_warehouse", targetWarehouse)
            .put("target_series", targetSeries)
            .put("reserve", reserve)
    }

    private fun fillSource(value: Struct, result: Struct) {
        if (value.getInt32("VKTSC0") != MARK_PRODUCT) {
            return
        }

        val reserve = value.getInt32("VKTSC1") == MARK_RESERVE

        val warehouse = when {
            reserve -> "KTSC2"
            else -> "KTSC1"
        }

        val series = when {
            reserve -> "KTSC3"
            else -> "KTSC2"
        }

        val seriesValue = value.getString(series).trim()

        val sourceProduct = value.getString("KTSC0").trim().toInt(INT_RADIX)
        val sourceWarehouse = value.getString(warehouse).trim().toInt(INT_RADIX)
        val sourceSeries = if (seriesValue.isNotEmpty()) {
            seriesValue.toInt(INT_RADIX)
        } else {
            0
        }

        result
            .put("source_product", sourceProduct)
            .put("source_warehouse", sourceWarehouse)
            .put("source_series", sourceSeries)
            .put("reserve", reserve)
    }

    private fun Double.round(decimals: Int): Double {
        return this.toBigDecimal().setScale(decimals, RoundingMode.HALF_UP).toDouble()
    }
}
