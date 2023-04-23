# Kafka Connect Single Message Transforms

This is an implementation of the [Kafka Connect SMT](https://docs.confluent.io/platform/current/connect/transforms/overview.html) interface 
to provide required transformations for 1C 7.7 data structs.

## Build

The library uses Gradle to build the JAR.

1. Install latest Java SDK.
2. Checkout the Git repository and change to its root folder.
3. Execute `./gradlew build`

The JAR can then be found in the `build/libs/` subfolder.

## Install

After the JAR was build as described above, copy it to your Kafka Connect instance into one of the directories listed in
the `plugin.path` property in the connect worker configuration file.
> Make sure to do this on all Kafka Connect worker nodes!

See the [Confluent Kafka Connect Plugins Userguide](https://docs.confluent.io/home/connect/self-managed/userguide.html#installing-kconnect-plugins) for more details.

## Example

### Connector Configuration

```json
{
  "name": ...,
  "config": {
    ...,
    "transforms": "trim,substring,radix",
    "transforms.trim.type": "org.alpaka.kafka.TrimTransform$Value",
    "transforms.trim.fields": "comma,separated,list,of,fields",
    "transforms.substring.type": "org.alpaka.kafka.SubstringTransform$Value",
    "transforms.substring.fields": "foo:1:3,bar:2",
    "transforms.radix.type": "org.alpaka.kafka.RadixTransform$Value",
    "transforms.radix.fields": "foo:16,bar:36"
  }
}
```

> Note that the transformer only supports some kind of structured input. So make sure that there is a [converter](https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/) class (e.g. [AvroConverter](https://www.confluent.io/hub/confluentinc/kafka-connect-avro-converter), [JsonConverter](https://www.confluent.io/hub/confluentinc/kafka-connect-json-schema-converter)) like `"value.converter": "io.confluent.connect.avro.AvroConverter"` that provides the data in a structured format.
