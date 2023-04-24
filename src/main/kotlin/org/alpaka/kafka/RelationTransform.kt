package org.alpaka.kafka

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements

class RelationTransform<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        const val OVERVIEW_DOC = "Transforms internal 1C 7.7 CRDOC information to relation table format"

        val CONFIG_DEF: ConfigDef = ConfigDef()

        private const val PURPOSE = "relation-convert"
        private const val PARENT_START = 6
        private const val PARENT_END = 15
        private const val INT_RADIX = 36

        private val keySchema = SchemaBuilder.struct()
            .name("relation-key")
            .doc("Relation key")
            .version(1)
            .field("id", Schema.INT32_SCHEMA)
            .build()

        private val valueSchema = SchemaBuilder.struct()
            .name("relation-value")
            .doc("Relation value")
            .version(1)
            .field("id", Schema.INT32_SCHEMA)
            .field("child", Schema.INT32_SCHEMA)
            .field("parent", Schema.INT32_SCHEMA)
            .field("section", Schema.INT32_SCHEMA)
            .build()
    }

    @Suppress("EmptyFunctionBlock")
    override fun configure(props: Map<String?, *>?) {
    }

    override fun apply(record: R): R = when {
        operatingValue(record) == null -> {
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
        val value = convert(Requirements.requireStruct(record?.key(), PURPOSE))
        val key = Struct(keySchema)
            .put("id", value.getInt32("ROW_ID"))

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
        val key = Struct(keySchema)
            .put("id", value.getInt32("id"))

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

    private fun operatingValue(record: R): Any? = record?.value()

    private fun convert(value: Struct): Struct
    {
        val childPart = value.getString("CHILDID").trim()
        val parentPart = value.getString("PARENTVAL").substring(PARENT_START, PARENT_END).trim()

        var child = 0
        var parent = 0

        if (childPart.isNotEmpty()) {
            child = childPart.toInt(INT_RADIX)
        }

        if (parentPart.isNotEmpty()) {
            parent = parentPart.toInt(INT_RADIX)
        }

        return Struct(valueSchema)
            .put("id", value.getInt32("ROW_ID"))
            .put("child", child)
            .put("parent", parent)
            .put("section", value.getInt32("MDID"))
    }
}
