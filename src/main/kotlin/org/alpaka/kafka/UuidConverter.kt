package org.alpaka.kafka

import io.debezium.spi.converter.CustomConverter
import io.debezium.spi.converter.CustomConverter.ConverterRegistration
import io.debezium.spi.converter.RelationalColumn
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.transforms.util.SimpleConfig
import org.postgresql.util.PGobject
import java.nio.ByteBuffer
import java.util.*

class UuidConverter: CustomConverter<SchemaBuilder, RelationalColumn> {
    private lateinit var columns: List<String>

    private val CONFIG_DEF: ConfigDef = ConfigDef()
        .define(
            "columns",
            ConfigDef.Type.LIST,
            listOf<String>(),
            ConfigDef.Importance.HIGH,
            "Name of columns to convert"
        )

    override fun configure(properties: Properties) {
        val config = SimpleConfig(CONFIG_DEF, properties)

        columns = config.getList("columns")
    }

    override fun converterFor(column: RelationalColumn, registration: ConverterRegistration<SchemaBuilder>) {
        val name = column.dataCollection() + '.' + column.name()

        if (!columns.contains(name)) {
            return
        }

        var schema = SchemaBuilder.string()
            .name("io.debezium.data.Uuid")
            .version(1)

        if (column.isOptional) {
            schema = schema.optional()
        }

        registration.register(schema, fun (x): String? {
            val data = when (x) {
                is PGobject -> {
                    x.value?.toByteArray()
                }

                is String -> {
                    x.toByteArray()
                }

                else -> {
                    x as ByteArray?
                }
            }

            if (data == null) {
                return null
            }

            val buffer = ByteBuffer.wrap(data)

            return UUID(buffer.getLong(), buffer.getLong()).toString()
        })
    }
}
