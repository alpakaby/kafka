package org.alpaka.kafka

import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.apache.kafka.connect.transforms.util.SimpleConfig
import java.text.SimpleDateFormat
import java.util.*

@Suppress("TooManyFunctions")
abstract class DateTimeIdDocTransform<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        @Suppress("unused")
        const val OVERVIEW_DOC = "Convert internal 1C 7.7 DATE_TIME_IDDOC field to DateTime."

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(
                "field",
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "Name of field to convert"
            )
            .define(
                "timezone",
                ConfigDef.Type.STRING,
                "UTC",
                ConfigDef.Importance.LOW,
                "Timezone to parse"
            )

        private val cache = SynchronizedCache(LRUCache<Schema, Schema>(16))

        private const val PURPOSE = "date-time-iddoc-convert"

        private const val DATE_PART_START = 0
        private const val DATE_PART_END = 8
        private const val TIME_PART_END = 14
        private const val TIME_RADIX = 36
        private const val TIME_ROUND = 10000
    }

    private val formatter = SimpleDateFormat("yyyyMMdd")

    private lateinit var _field: String

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)

        _field = config.getString("field")

        formatter.timeZone = TimeZone.getTimeZone(config.getString("timezone"))
    }

    override fun apply(record: R): R = when {
        operatingValue(record) == null -> {
            record
        }
        operatingSchema(record) == null -> {
            applySchemaless(record)
        }
        else -> {
            applyWithSchema(record)
        }
    }

    @Suppress("EmptyFunctionBlock")
    override fun close() {
    }

    override fun config(): ConfigDef = CONFIG_DEF

    protected abstract fun operatingSchema(record: R?): Schema?
    protected abstract fun operatingValue(record: R?): Any?
    protected abstract fun newRecord(record: R?, schema: Schema?, value: Any?): R

    private fun applySchemaless(record: R): R {
        val value = Requirements.requireMap(operatingValue(record), PURPOSE)

        if (value.containsKey(_field) && value[_field] is String) {
            value[_field] = convert(value[_field] as String).time
        }

        return newRecord(record, null, value)
    }

    private fun applyWithSchema(record: R): R {
        val value = Requirements.requireStruct(operatingValue(record), PURPOSE)
        val schema = operatingSchema(record) ?: return record

        val outputSchema = copySchema(schema)
        val outputValue = Struct(outputSchema)

        for (field in schema.fields()) {
            val name = field.name()

            if (name == _field) {
                outputValue.put(name, convert(value.getString(name)))
            } else {
                outputValue.put(name, value.get(name))
            }
        }

        return newRecord(record, outputSchema, outputValue)
    }

    private fun convert(value: String): Date {
        val datePart = value.substring(DATE_PART_START, DATE_PART_END).trim()
        val timePart = value.substring(DATE_PART_END, TIME_PART_END).trim().toLong(TIME_RADIX) / TIME_ROUND

        val date = formatter.parse(datePart).toInstant()

        return Date(date.plusSeconds(timePart).toEpochMilli())
    }

    private fun copySchema(schema: Schema): Schema {
        val cached = cache.get(schema)

        if (cached != null) {
            return cached
        }

        val output = SchemaUtil.copySchemaBasics(schema)

        for (field in schema.fields()) {
            val name = field.name()

            if (name == _field) {
                output.field(name, Timestamp.SCHEMA)
            } else {
                output.field(name, field.schema())
            }
        }

        cache.put(schema, output)

        return output
    }

    class Key<R : ConnectRecord<R>?> : DateTimeIdDocTransform<R>() {
        override fun operatingSchema(record: R?): Schema? = record?.keySchema()

        override fun operatingValue(record: R?): Any? = record?.key()

        override fun newRecord(record: R?, schema: Schema?, value: Any?): R = record!!.newRecord(
            record.topic(),
            record.kafkaPartition(),
            schema,
            value,
            record.valueSchema(),
            record.value(),
            record.timestamp()
        )
    }

    class Value<R : ConnectRecord<R>?> : DateTimeIdDocTransform<R>() {
        override fun operatingSchema(record: R?): Schema? = record?.valueSchema()

        override fun operatingValue(record: R?): Any? = record?.value()

        override fun newRecord(record: R?, schema: Schema?, value: Any?): R = record!!.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            schema,
            value,
            record.timestamp()
        )
    }
}
