package org.alpaka.kafka

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.CustomConverter.ConverterRegistration;
import io.debezium.spi.converter.RelationalColumn;

import org.apache.kafka.connect.data.SchemaBuilder;
import java.nio.charset.StandardCharsets
import java.util.*

class MCharConverter: CustomConverter<SchemaBuilder, RelationalColumn> {
    private val decoder = Base64.getDecoder()

    override fun configure(properties: Properties) {
    }

    override fun converterFor(column: RelationalColumn, registration: ConverterRegistration<SchemaBuilder>) {
        if ("mvarchar" == column.typeName()) {
            var schema = SchemaBuilder.string()

            if (column.isOptional) {
                schema = schema.optional()
            }

            registration.register(schema, fun (x): String? {
                if (x == null) {
                    return null
                }

                return String(x as ByteArray, StandardCharsets.UTF_8);
            });
        }
    }
}
