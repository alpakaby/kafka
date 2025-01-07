package org.alpaka.kafka

import io.debezium.spi.converter.CustomConverter
import io.debezium.spi.converter.CustomConverter.ConverterRegistration
import io.debezium.spi.converter.RelationalColumn
import org.apache.kafka.connect.data.SchemaBuilder
import org.postgresql.util.PGobject
import java.nio.ByteBuffer
import java.util.*

class UuidConverter: CustomConverter<SchemaBuilder, RelationalColumn> {
    private val schema = SchemaBuilder.string()
        .name("io.debezium.data.Uuid")
        .version(1)

    private val columns = arrayOf("_idrref", "_parentidrref")

    override fun configure(properties: Properties) {
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
