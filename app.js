var kafka = require('kafka-node'),
    Promise = require('promise'),
    config = require('config-node')(),

    clientId = config.clientId,

    client = new kafka.Client(config.kafka.zkConnect, clientId),

    consumer = new kafka.HighLevelConsumer(
        client, [{
            topic: config.kafka.topic
        }], {
        //consumer group id
        groupId: clientId,
        // Auto commit config 
        autoCommit: false,
        autoCommitIntervalMs: 5000,
        // The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued, default 100ms 
        fetchMaxWaitMs: 100,
        // This is the minimum number of bytes of messages that must be available to give a response, default 1 byte 
        fetchMinBytes: 1,
        // The maximum bytes to include in the message set for this partition. This helps bound the size of the response. 
        fetchMaxBytes: 1024 * 10,
        // If set true, consumer will fetch message from the given offset in the payloads 
        fromOffset: false,
        // If set to 'buffer', values will be returned as raw buffer objects. 
        encoding: 'utf8'
    }),

    exit = function () {
        consumer.close(function () {
            process.exit();
        });
    };


consumer.on('message', function (m) {
    // todo: update redis
    console.log(m);
});

consumer.on('error', function (e) {
    log.error('Error receiving messages from source: ' + e);
    exit();
});

consumer.on('offsetOutOfRange', function (e) {
    log.error('Error receiving messages from source: ' + e);
    exit();
});

process.on('SIGINT', function () {
    log.info('Shutting down...');
    exit();
});
