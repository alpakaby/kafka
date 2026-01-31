package org.alpaka.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.uuid.Generators
import com.fasterxml.uuid.impl.NameBasedGenerator
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
import java.util.*

@Suppress("TooManyFunctions")
abstract class InsertUuid<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        @Suppress("unused")
        const val OVERVIEW_DOC = "Insert UUID based on specified fields"

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(
                "fields",
                ConfigDef.Type.LIST,
                ConfigDef.Importance.HIGH,
                "List of fields to base UUID"
            )
            .define(
                "output",
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "Output field"
            )
            .define(
                "namespace",
                ConfigDef.Type.STRING,
                null,
                UuidValidator(),
                ConfigDef.Importance.LOW,
                "UUID v5 namespace",
            )

        private val cache = SynchronizedCache(LRUCache<Schema, Schema>(16))
        private val mapper = ObjectMapper()

        private const val PURPOSE = "onliner-kafka-smt-uuid"
    }

    private lateinit var _fields: List<String>
    private lateinit var _output: String
    private lateinit var _namespace: NameBasedGenerator

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)
        val namespace = config.getString("namespace")?.let(UUID::fromString) ?: NameBasedGenerator.NAMESPACE_URL

        _namespace = Generators.nameBasedGenerator(namespace)
        _fields = config.getList("fields")
        _output = config.getString("output")
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
        val bytes = mutableListOf<Byte>()

        for (field in _fields) {
            if (!value.containsKey(field)) {
                continue
            }

            bytes.addAll(convert(value[field]))
        }

        value[_output] = _namespace.generate(bytes.toByteArray()).toString()

        return newRecord(record, null, value)
    }

    private fun applyWithSchema(record: R): R {
        val value = Requirements.requireStruct(operatingValue(record), PURPOSE)
        val schema = operatingSchema(record) ?: return record
        val bytes = mutableListOf<Byte>()

        val outputSchema = copySchema(schema)
        val outputValue = Struct(outputSchema)

        for (field in schema.fields()) {
            if (_fields.contains(field.name())) {
                bytes.addAll(convert(value.get(field)))
            }

            outputValue.put(field.name(), value.get(field))
        }

        outputValue.put(_output, _namespace.generate(bytes.toByteArray()).toString())

        return newRecord(record, outputSchema, outputValue)
    }

    private fun convert(value: Any?): Iterable<Byte> {
        return mapper.writeValueAsBytes(value).asIterable()
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

        output.field(_output, SchemaBuilder.STRING_SCHEMA)

        cache.put(schema, output)

        return output
    }

    class Key<R : ConnectRecord<R>?> : InsertUuid<R>() {
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

    class Value<R : ConnectRecord<R>?> : InsertUuid<R>() {
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

    class UuidValidator : ConfigDef.Validator {
        companion object {
            private val UUID_REGEX = Regex(
                "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
            )
        }

        override fun ensureValid(name: String, value: Any?) {
            if (value != null && !UUID_REGEX.matches(value.toString())) {
                throw ConfigException(name, value, "Must be a valid UUID")
            }
        }

        override fun toString(): String = "UUID"
    }
}
