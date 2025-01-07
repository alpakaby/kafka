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
    private val schema = SchemaBuilder.string()
        .name("io.debezium.data.Uuid")
        .version(1)

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
        val configured = config.getList("columns")

        columns = listOf("_idrref", "_parentidrref") + configured
    }

    override fun converterFor(column: RelationalColumn, registration: ConverterRegistration<SchemaBuilder>) {
        if (!columns.contains(column.name())) {
            return
        }

        registration.register(schema, fun (x): String {
            val data = if (x is PGobject) {
                x.value?.toByteArray()
            } else if (x is String) {
                x.toByteArray()
            } else {
                x as ByteArray?
            }

            val buffer = ByteBuffer.wrap(data)

            return UUID(buffer.getLong(), buffer.getLong()).toString()
        })
    }
}
