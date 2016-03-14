var _ = require('lodash'),
    Bacon = require('baconjs'),
    amqp = require('amqp'),
    uuid = require('node-uuid'),
    murmurhash = require('murmurhash');

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
        STATUS_OK = "ok",
        STATUS_ERROR = "error",
        DEFAULT_RPC_CLIENT_RESPONSE_TIMEOUT = 3000,
        DEFAULT_RPC_INVOCATION_RESPONSE_TIMEOUT = 3000,
        DEFAULT_NAMESPACE_ID = "shibuya",
        CLIENT_ID = uuid.v4();

    var nameTemplate = function(name, id){
        return _.compact([DEFAULT_NAMESPACE_ID, name, id]).join('.');
    };

    // Global configuration
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

    // Global connection
    var connectionProperty = (function(connection){
        return Bacon
            .fromEvent(connection, 'ready', _.constant(connection))
            .take(1)
            .toProperty();
    })(amqp.createConnection(
        config.connection,
        {
            reconnect: true,
            reconnectBackoffStrategy: AMQP_DEFAULT_RECONNECT_STRATEGY,
            reconnectBackoffTime: AMQP_DEFAULT_RECONNECT_BACKOFF_TIME
        }
    ));

    // Global exchange
    var exchangeProperty = connectionProperty.flatMap(function(connection){
        return Bacon.fromCallback(connection.exchange.bind(
            connection,
            nameTemplate('exchange'),
            {
                autoDelete: true,
                type: AMQP_EXCHANGE_TYPE_TOPIC
            }
        ));
    }).toProperty();

    return {
        registerService: (function(receiver){
            Bacon.combineTemplate({
                connection: connectionProperty,
                exchange: exchangeProperty,
                params: Bacon.fromBinder(function(sink){
                    receiver = function(name, service){
                        sink({
                            serviceId: nameTemplate('service', murmurhash.v3(name).toString()),
                            service: service
                        });
                    };
                })
            })
            .flatMap(function(params){
                var serviceId = params.params.serviceId,
                    service = params.params.service,
                    connection = params.connection,
                    exchange = params.exchange;

                return Bacon
                    .fromCallback(connection.queue.bind(connection, serviceId, { autoDelete: true }))
                    .flatMap(function(queue){
                        return Bacon
                            .fromCallback(queue.bind.bind(queue, exchange.name, serviceId))
                            .flatMap(function(){
                                return Bacon
                                    .fromBinder(
                                        _.flow(
                                            function(sink){
                                                return function(message, header, delivery){
                                                    sink(delivery["contentType"] === "application/json" && message && _.assign(
                                                        {
                                                            method: message["method"],
                                                            parameters: message["parameters"]
                                                        },
                                                        _.pick(delivery, 'correlationId', 'replyTo')
                                                    ));
                                                };
                                            },
                                            queue.subscribe.bind(queue, { ack: false })
                                        )
                                    )
                                    .filter(Boolean);
                            });
                    })
                    .flatMap(function(invocationRequest){
                        return Bacon.fromNodeCallback(function(cb){
                            try {
                                service[invocationRequest["method"]].apply(service, _.flatten([invocationRequest["parameters"], cb]));
                            } catch(e) {
                                cb(e.toString());
                            }
                        })
                        .merge(Bacon.later(DEFAULT_RPC_INVOCATION_RESPONSE_TIMEOUT, new Bacon.Error('Invocation did not yield a response in a timely fashion')))
                        .map(function(result){
                            return {
                                status: STATUS_OK,
                                result: result
                            };
                        })
                        .mapError(function(error){
                            return {
                                status: STATUS_ERROR,
                                result: error
                            };
                        })
                        .take(1)
                        .map(function(message){
                            return exchange.publish.bind(
                                exchange,
                                invocationRequest.replyTo,
                                message,
                                {
                                    deliveryMode: AMQP_EXCHANGE_DELIVERY_MODE_NON_PERSISTENT,
                                    correlationId: invocationRequest.correlationId
                                }
                            );
                        });
                    });
            })
            .onValue('.call');

            return receiver;
        })(),
        callService: (function(receiver){
            Bacon.combineTemplate({
                connection: connectionProperty,
                exchange: exchangeProperty,
                queue: connectionProperty.flatMap(function(connection){
                    return Bacon.fromCallback(
                        connection.queue.bind(
                            connection,
                            nameTemplate('feedback', murmurhash.v3(CLIENT_ID).toString()),
                            { exclusive: true }
                        )
                    )
                    .combine(exchangeProperty, function(queue, exchange){
                        return {
                            queue: queue,
                            exchange: exchange
                        };
                    })
                    .flatMap(function(opts){
                        return Bacon.fromCallback(opts.queue.bind.bind(opts.queue, opts.exchange.name, opts.queue.name)).map(opts.queue);
                    });
                }).toProperty(),
                params: Bacon.fromBinder(function(sink){
                    receiver = function(name, method, properties, callback){
                        sink({
                            serviceId: nameTemplate('service', murmurhash.v3(name).toString()),
                            method: method,
                            properties: properties || [],
                            callback: callback || _.noop
                        });
                    };
                })
            })
            .log();

            return receiver;
        })()
    };
};