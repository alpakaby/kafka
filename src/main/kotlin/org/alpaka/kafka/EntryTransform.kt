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
        } else -> {
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

    private fun convert(value: Struct): Struct
    {
        val id = value.getInt32("ROW_ID")
        val date = inputFormatter.parse(value.getString("DATE_TIME_DOCID").substring(DATE_PART_START, DATE_PART_END))
        var reserve = false
        val qty = value.getFloat64("AMOUNT").toInt()
        val price = when (qty) {
            0 -> 0.0
            else -> value.getFloat64("SUM_").absoluteValue / qty.absoluteValue
        }
        var sourceProduct: Int? = null
        var sourceWarehouse: Int? = null
        var sourceSeries: Int? = null

        var targetProduct: Int? = null
        var targetWarehouse: Int? = null
        var targetSeries: Int? = null

        if (value.getInt32("VKTSC0") == MARK_PRODUCT) {
            reserve = value.getInt32("VKTSC1") == MARK_RESERVE

            val warehouse = when {
                reserve -> "KTSC2"
                else -> "KTSC1"
            }

            sourceProduct = value.getString("KTSC0").trim().toInt(INT_RADIX)
            sourceWarehouse = value.getString(warehouse).trim().toInt(INT_RADIX)

            val series = value.getString("KTSC3").trim();

            sourceSeries = if (series.isNotEmpty()) {
                series.toInt(INT_RADIX)
            } else {
                0
            }
        }

        if (value.getInt32("VDTSC0") == MARK_PRODUCT) {
            reserve = value.getInt32("VDTSC1") == MARK_RESERVE

            val warehouse = when {
                reserve -> "DTSC2"
                else -> "DTSC1"
            }

            targetProduct = value.getString("DTSC0").trim().toInt(INT_RADIX)
            targetWarehouse = value.getString(warehouse).trim().toInt(INT_RADIX)

            val series = value.getString("DTSC3").trim();

            targetSeries = if (series.isNotEmpty()) {
                series.toInt(INT_RADIX)
            } else {
                0
            }
        }

        return Struct(valueSchema)
            .put("id", id)
            .put("date", date)
            .put("source_product", sourceProduct)
            .put("source_warehouse", sourceWarehouse)
            .put("source_series", sourceSeries)
            .put("target_product", targetProduct)
            .put("target_warehouse", targetWarehouse)
            .put("target_series", targetSeries)
            .put("qty", qty)
            .put("price", price.round(2).toFloat())
            .put("reserve", reserve)
    }

    private fun Double.round(decimals: Int): Double {
        return this.toBigDecimal().setScale(decimals, RoundingMode.HALF_UP).toDouble()
    }
}
