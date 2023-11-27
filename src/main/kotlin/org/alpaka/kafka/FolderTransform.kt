package org.alpaka.kafka

import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.apache.kafka.connect.transforms.util.SimpleConfig

@Suppress("TooManyFunctions")
abstract class FolderTransform<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        const val OVERVIEW_DOC = "Convert integer is_folder field to bool value."

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(
                "field",
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "Name of field to convert"
            )

        private val cache = SynchronizedCache(LRUCache<Schema, Schema>(16))

        private const val PURPOSE = "folder-convert"
    }

    private lateinit var _field: String

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)

        _field = config.getString("field")
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

        if (value.containsKey(_field) && value[_field] is Int) {
            value[_field] = convert(value[_field] as Int)
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
                outputValue.put(name, convert(value.getInt32(name)))
            } else {
                outputValue.put(name, value.get(name))
            }
        }

        return newRecord(record, outputSchema, outputValue)
    }

    private fun convert(value: Int): Boolean {
        return value != 2
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
                output.field(name, Schema.BOOLEAN_SCHEMA)
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
