console.log("consumer");
const kafka = require('node-rdkafka');

// Create a Kafka consumer
const consumer = new kafka.KafkaConsumer({
    "metadata.broker.list": "127.0.0.1:9092",
    "group.id": "my-group",
    "fetch.wait.max.ms": 10,
    "fetch.error.backoff.ms": 50,
    "auto.offset.reset": "earliest"
}, {});

consumer.connect();

consumer.on('ready', function () {
    console.log("consumer is ready");
    consumer.subscribe(['my-topic']);
    consumer.consume();
}).on('data', (data) => {
    console.log("Received data: " + data.value);
});