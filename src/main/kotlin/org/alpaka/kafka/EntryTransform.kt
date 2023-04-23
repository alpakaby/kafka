package org.alpaka.kafka

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
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
        val OVERVIEW_DOC = "Transforms internal 1C 7.7 entries information to leftover table format"
        val CONFIG_DEF: ConfigDef = ConfigDef()

        private const val PURPOSE = "entry-convert"
    }

    private val inputFormatter = SimpleDateFormat("yyyyMMdd")
    private val outputFormatter = SimpleDateFormat("yyyy-MM-dd")

    override fun configure(props: Map<String?, *>?) {
        inputFormatter.timeZone = TimeZone.getTimeZone("UTC")
    }

    override fun apply(record: R): R = when {
        operatingValue(record) == null -> {
            record
        }
        else -> {
            applyWithSchema(record)
        }
    }

    @Suppress("EmptyFunctionBlock")
    override fun close() {
    }

    override fun config(): ConfigDef = CONFIG_DEF

    private fun applyWithSchema(record: R): R {
        val value = Requirements.requireStruct(operatingValue(record), PURPOSE)

        return newRecord(record, convert(value))
    }

    private fun newRecord(record: R, updatedValue: Struct): R = record!!.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        updatedValue.schema(),
        updatedValue,
        record.timestamp()
    )

    private fun operatingValue(record: R): Any? = record?.value()

    private fun convert(value: Struct): Struct
    {
        val schema = SchemaBuilder.struct()
            .name("entry")
            .doc("Entry information")
            .version(1)
            .field("id", Schema.INT32_SCHEMA)
            .field("date", Schema.STRING_SCHEMA)
            .field("product", Schema.INT32_SCHEMA)
            .field("source", Schema.OPTIONAL_INT32_SCHEMA)
            .field("target", Schema.OPTIONAL_INT32_SCHEMA)
            .field("qty", Schema.INT32_SCHEMA)
            .field("price", Schema.FLOAT32_SCHEMA)
            .field("reserve", Schema.BOOLEAN_SCHEMA)
            .build()

        val id = value.getInt32("ROW_ID")
        val date = inputFormatter.parse(value.getString("DATE_TIME_DOCID").substring(0, 8))
        var reserve = false
        val qty = value.getFloat64("AMOUNT").absoluteValue.toInt()
        val price = value.getFloat64("SUM_").absoluteValue / qty
        var product = 0
        var source: Int? = null
        var target: Int? = null

        if (value.getInt32("VKTSC0") == 525) {
            reserve = value.getInt32("VKTSC1") == 53074

            val warehouse = when {
                reserve -> "KTSC2"
                else -> "KTSC1"
            }

            product = value.getString("KTSC0").trim().toInt(36)
            source = value.getString(warehouse).trim().toInt(36)
        }

        if (value.getInt32("VDTSC0") == 525) {
            reserve = value.getInt32("VDTSC1") == 53074

            val warehouse = when {
                reserve -> "DTSC2"
                else -> "DTSC1"
            }

            product = value.getString("DTSC0").trim().toInt(36)
            target = value.getString(warehouse).trim().toInt(36)
        }

        return Struct(schema)
            .put("id", id)
            .put("date", outputFormatter.format(date))
            .put("product", product)
            .put("source", source)
            .put("target", target)
            .put("qty", qty)
            .put("price", price.round(2).toFloat())
            .put("reserve", reserve)
    }

    private fun Double.round(decimals: Int): Double {
        return this.toBigDecimal().setScale(decimals, RoundingMode.HALF_UP).toDouble()
    }
}
