var _ = require('lodash'),
    Bacon = require('baconjs'),
    amqp = require('amqp'),
    uuid = require('node-uuid');

module.exports = function(userConfiguration){

    var AMQP_AUTH_MECHANISM_PLAIN = "AMQPLAIN",
        AMQP_EXCHANGE_TYPE_TOPIC = "topic",
        AMQP_EXCHANGE_DELIVERY_MODE_NON_PERSISTENT = 1,
        AMQP_RECONNECT_STRATEGY_LINEAR = "linear",
        AMQP_DEFAULT_AUTH_MECHANISM = AMQP_AUTH_MECHANISM_PLAIN,
        AMQP_DEFAULT_PORT = 5672,
        AMQP_DEFAULT_VHOST = "/",
        AMQP_DEFAULT_CONNECTION_TIMEOUT = 1000,
        AMQP_DEFAULT_HEARTBEAT_INTERVAL = 60,
        AMQP_DEFAULT_RECONNECT_STRATEGY = AMQP_RECONNECT_STRATEGY_LINEAR,
        AMQP_DEFAULT_RECONNECT_BACKOFF_TIME = 10000,
        DEFAULT_RPC_CLIENT_RESPONSE_TIMEOUT = 3000,
        DEFAULT_RPC_SERVER_RESPONSE_TIMEOUT = 3000,
        DEFAULT_CLIENT_ID = uuid.v4(),
        DEFAULT_ROOT_NAMESPACE = "Shibuya";

    var config = _.defaultsDeep(userConfiguration, {
        connection: {
            heartbeat: AMQP_DEFAULT_HEARTBEAT_INTERVAL,
            port: AMQP_DEFAULT_PORT,
            connectionTimeout: AMQP_DEFAULT_CONNECTION_TIMEOUT,
            authMechanism: AMQP_DEFAULT_AUTH_MECHANISM,
            vhost: AMQP_DEFAULT_VHOST,
            noDelay: true
        }
    });

    var connection = amqp.createConnection(
        config.connection,
        {
            reconnect: true,
            reconnectBackoffStrategy: AMQP_DEFAULT_RECONNECT_STRATEGY,
            reconnectBackoffTime: AMQP_DEFAULT_RECONNECT_BACKOFF_TIME
        }
    );
};