package org.alpaka.kafka

import io.debezium.spi.converter.CustomConverter
import io.debezium.spi.converter.CustomConverter.ConverterRegistration
import io.debezium.spi.converter.RelationalColumn
import org.apache.kafka.connect.data.SchemaBuilder
import java.nio.charset.StandardCharsets
import java.util.*
import org.postgresql.util.PGobject

class MCharConverter: CustomConverter<SchemaBuilder, RelationalColumn> {
    override fun configure(properties: Properties) {
    }

    override fun converterFor(column: RelationalColumn, registration: ConverterRegistration<SchemaBuilder>) {
        if ("mvarchar" == column.typeName()) {
            var schema = SchemaBuilder.string()

            if (column.isOptional) {
                schema = schema.optional()
            }

            registration.register(schema, fun (x): String? {
                when (x) {
                    is PGobject -> {
                        return x.value?.let { String(it.toByteArray(), StandardCharsets.UTF_8) }
                    }

                    is String -> {
                        return String(x.toByteArray(), StandardCharsets.UTF_8)
                    }

                    is ByteArray -> {
                        return String(x, StandardCharsets.UTF_8)
                    }

                    else -> {
                        return null
                    }
                }
            });
        }
    }
}
