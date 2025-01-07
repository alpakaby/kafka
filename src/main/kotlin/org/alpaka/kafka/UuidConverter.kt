package org.alpaka.kafka

import io.debezium.spi.converter.CustomConverter
import io.debezium.spi.converter.CustomConverter.ConverterRegistration
import io.debezium.spi.converter.RelationalColumn
import org.apache.kafka.connect.data.SchemaBuilder
import java.nio.ByteBuffer
import java.util.*

class UuidConverter: CustomConverter<SchemaBuilder, RelationalColumn> {
    private val decoder = Base64.getDecoder()

    override fun configure(properties: Properties) {
    }

    override fun converterFor(column: RelationalColumn, registration: ConverterRegistration<SchemaBuilder>) {
        if ("_idrref" == column.name()) {
            registration.register(SchemaBuilder.string(), fun (x): String {
                val buffer = ByteBuffer.wrap(decoder.decode(x.toString()));

                return UUID(buffer.getLong(), buffer.getLong()).toString()
            });
        }
    }
}
