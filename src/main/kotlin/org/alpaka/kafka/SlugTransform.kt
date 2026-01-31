package org.alpaka.kafka

import com.ibm.icu.text.Transliterator
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.apache.kafka.connect.transforms.util.SimpleConfig
import java.util.Locale

@Suppress("TooManyFunctions")
abstract class SlugTransform<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        @Suppress("unused")
        const val OVERVIEW_DOC = "Insert slug based on specified field"

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(
                "input",
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                "Field to generate slug from"
            )
            .define(
                "output",
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                "Output field to store slug"
            )

        private val cache = SynchronizedCache(LRUCache<Schema, Schema>(16))

        private val transliterator = Transliterator.getInstance("Any-Latin; Latin-ASCII")
        private val REGEX_CLEAN = "[^a-zA-Z0-9\\s-]".toRegex()
        private val REGEX_SPACE = "\\s+".toRegex()

        private const val PURPOSE = "alpaka-kafka-smt-slug"
    }

    private lateinit var _input: String
    private lateinit var _output: String

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)

        _input = config.getString("input")
        _output = config.getString("output")

        if (_input == _output) {
            throw ConfigException(
                "output",
                _output,
                "output field must be different from input field"
            )
        }
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

    override fun close() = Unit
    override fun config(): ConfigDef = CONFIG_DEF

    protected abstract fun operatingSchema(record: R?): Schema?
    protected abstract fun operatingValue(record: R?): Any?
    protected abstract fun newRecord(record: R?, schema: Schema?, value: Any?): R

    private fun applySchemaless(record: R): R {
        val value = Requirements.requireMap(operatingValue(record), PURPOSE)

        if (value.containsKey(_input)) {
            value[_output] = convert(value[_input])
        }

        return newRecord(record, null, value)
    }

    private fun applyWithSchema(record: R): R {
        val value = Requirements.requireStruct(operatingValue(record), PURPOSE)
        val schema = operatingSchema(record) ?: return record

        val outputSchema = copySchema(schema)
        val outputValue = Struct(outputSchema)

        for (field in schema.fields()) {
            val fieldValue = value.get(field)

            if (_input == field.name()) {
                outputValue.put(_output, convert(fieldValue))
            }

            outputValue.put(field.name(), fieldValue)
        }

        return newRecord(record, outputSchema, outputValue)
    }

    private fun convert(value: Any?): String? {
        if (value == null) return null

        val latin = transliterator.transliterate(value.toString())

        return latin
            .replace(REGEX_CLEAN, "")
            .replace(REGEX_SPACE, "-")
            .lowercase(Locale.ROOT)
    }

    private fun copySchema(schema: Schema): Schema {
        val cached = cache.get(schema)

        if (cached != null) {
            return cached
        }

        val output = SchemaUtil.copySchemaBasics(schema)

        for (field in schema.fields()) {
            output.field(field.name(), field.schema())
        }

        if (schema.field(_output) == null) {
            output.field(_output, SchemaBuilder.STRING_SCHEMA)
        }

        cache.put(schema, output)

        return output
    }

    class Key<R : ConnectRecord<R>?> : SlugTransform<R>() {
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

    class Value<R : ConnectRecord<R>?> : SlugTransform<R>() {
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
