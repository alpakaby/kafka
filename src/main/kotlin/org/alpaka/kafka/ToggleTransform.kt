package org.alpaka.kafka

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SimpleConfig

@Suppress("TooManyFunctions")
abstract class ToggleTransform<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        @Suppress("unused")
        const val OVERVIEW_DOC = "Toggle value of boolean fields"

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(
                "fields",
                ConfigDef.Type.LIST,
                listOf<String>(),
                ConfigDef.Importance.HIGH,
                "Field to generate slug from"
            )

        private const val PURPOSE = "alpaka-kafka-smt-toggle"
    }

    private lateinit var _fields: Set<String>

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)

        _fields = config.getList("fields").toSet()
    }

    override fun apply(record: R): R = when {
        operatingValue(record) == null -> record
        operatingSchema(record) == null -> applySchemaless(record)
        else -> applyWithSchema(record)
    }

    override fun close() = Unit
    override fun config(): ConfigDef = CONFIG_DEF

    protected abstract fun operatingSchema(record: R?): Schema?
    protected abstract fun operatingValue(record: R?): Any?
    protected abstract fun newRecord(record: R?, schema: Schema?, value: Any?): R

    private fun applySchemaless(record: R): R {
        val value = Requirements.requireMap(operatingValue(record), PURPOSE).toMutableMap()

        for (field in _fields) {
            if (value.containsKey(field)) {
                val current = value[field]

                if (current is Boolean) {
                    value[field] = !current
                }
            }
        }

        return newRecord(record, null, value)
    }

    private fun applyWithSchema(record: R): R {
        val value = Requirements.requireStruct(operatingValue(record), PURPOSE)
        val schema = operatingSchema(record) ?: return record

        val outputValue = Struct(schema)

        for (field in schema.fields()) {
            val fieldName = field.name()
            val fieldSchema = field.schema()
            val fieldValue =
                if (
                    fieldName in _fields &&
                    fieldSchema.type() == Schema.Type.BOOLEAN
                ) {
                    val current = value.get(field)
                    if (current == null) {
                        null
                    } else {
                        !(current as Boolean)
                    }
                } else {
                    value.get(field)
                }

            outputValue.put(fieldName, fieldValue)
        }

        return newRecord(record, schema, outputValue)
    }

    class Key<R : ConnectRecord<R>?> : ToggleTransform<R>() {
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

    class Value<R : ConnectRecord<R>?> : ToggleTransform<R>() {
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
