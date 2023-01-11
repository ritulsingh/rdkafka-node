const kafka = require('node-rdkafka');

// Create a Kafka producer
const producer = new kafka.Producer({
    "metadata.broker.list": "127.0.0.1:9092",
    "group.id": "hello-group",
    // "fetch.wait.max.ms": 10,
    // "fetch.error.backoff.ms": 50,
    dr_cb: true
});
//logging debug messages, if debug is enabled
producer.on('event.log', function (log) {
    console.log(log);
});

producer.on('delivery-report', function (err, report) {
    console.log('delivery-report: ' + JSON.stringify(report));
});

var counter = 0;
var maxMessages = 3;

producer.on('ready', function (arg) {
    try {
        console.log('producer ready ' + JSON.stringify(arg));
        // producer.produce(
        //     'my-topic',
        //     null,
        //     Buffer.from('Awesome message'),
        //     Date.now()
        // );
        for (let i = 0; i < maxMessages; i++) {
            let value = Buffer.from('value-' +i);
            let key = "key-"+i;
            let partition = -1;
            producer.produce('my-topic', partition, value, key);
          }
          let pollLoop = setInterval(function() {
              producer.poll();
              if (counter === maxMessages) {
                clearInterval(pollLoop);
                producer.disconnect();
              }
            }, 1000);

    } catch (err) {
        console.error('A problem occurred when sending our message');
        console.error(err);
    }
});

// Any errors we encounter, including connection errors
producer.on('event.error', function (err) {
    console.error('Error from producer');
    console.error(err);
})

producer.on('disconnected', function (arg) {
    console.log('producer disconnected. ' + JSON.stringify(arg));
});

// Connect to the broker manually
producer.connect();


// Create a Kafka consumer
const consumer = new kafka.KafkaConsumer({
    "metadata.broker.list": "127.0.0.1:9092",
    "group.id": "my-group",
    // "fetch.wait.max.ms": 10,
    // "fetch.error.backoff.ms": 50,
    "auto.offset.reset": "earliest"
}, {});


consumer.on('ready', function () {
    console.log("consumer is ready");
    consumer.subscribe(['my-topic']);
    consumer.consume();
}).on('data', (data) => {
    console.log("Received data: " + data.value);
});
consumer.connect();