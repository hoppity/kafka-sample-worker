var kafka = require('kafka-node'),
    Promise = require('promise'),
    config = require('config-node')(),
    redis = require('redis'),
    bunyan = require('bunyan'),

    clientId = config.clientId,
    logLevel = config.logLevel || 'info',
    log = bunyan.createLogger({ name: clientId, level: logLevel }),

    kafkaClient = new kafka.Client(config.kafka.zkConnect, clientId),

    consumer = new kafka.HighLevelConsumer(
        kafkaClient, [{
            topic: config.kafka.topic
        }], {
        //consumer group id
        groupId: clientId,
        // Auto commit config 
        autoCommit: true,
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

    redisClient = redis.createClient(),

    exit = function () {
        consumer.close(function () {
            process.exit();
        });
    };

consumer.on('message', function (m) {
    log.debug(m, 'Received message.');
    var value = JSON.parse(m.value);
    redisClient.zadd([
        'stock:' + value.item.name,
        value._timestamp,
        value.item.price
    ], function (e, r) {
        if (!e) return;

        log.error(e);
        exit();
    })
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
