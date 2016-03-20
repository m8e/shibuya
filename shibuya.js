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

    // Rabbit entity template
    var nameTemplate = function(name, id){ return _.compact([DEFAULT_NAMESPACE_ID, name, id && murmurhash.v3(id).toString()]).join('.'); };

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

    // Create connection
    var connection = amqp.createConnection(
        config.connection,
        {
            reconnect: true,
            reconnectBackoffStrategy: AMQP_DEFAULT_RECONNECT_STRATEGY,
            reconnectBackoffTime: AMQP_DEFAULT_RECONNECT_BACKOFF_TIME
        }
    );

    // Define interface methods
    var interface = {};

    var registerStream = Bacon.fromBinder(function(sink){
        interface.register = function(name, instance, options){
            sink({ name: name, instance: instance, options: options });
        }
    }).onValue(_.noop);

    var callStream = Bacon.fromBinder(function(sink){
        interface.call = function(){
            var args = _.toArray(arguments),
                cb = args.pop() || _.noop,
                name = args.shift(),
                options = args.shift() || {};

            sink({ name: name, cb: cb, options: options });
        };
    }).onValue(_.noop);

    Bacon
        .fromEvent(connection, 'ready')
        .take(1)
        .map(connection)
        .flatMap(function(connection){
            return Bacon.combineTemplate({
                connection: connection,
                exchange: Bacon.fromCallback(connection.exchange.bind(connection,  nameTemplate('exchange'), { autoDelete: true, type: AMQP_EXCHANGE_TYPE_TOPIC })),
                feedbackQueue: Bacon.fromCallback(connection.queue.bind(connection, nameTemplate('feedback', CLIENT_ID), { exclusive: true }))
            });
        })
        .onValue(function(asset){
            var connection = asset["connection"],
                exchange = asset["exchange"],
                feedbackQueue = asset["feedbackQueue"];

            // Bind feedback queue to exchange (the queue name doubles as the routing key)
            feedbackQueue.bind(exchange.name, feedbackQueue.name);

            return registerStream
                .flatMap(function(registrationProperties){
                    var name = registrationProperties["name"],
                        instance = registrationProperties["instance"],
                        options = registrationProperties["options"];

                    return Bacon
                        .fromCallback(connection.queue.bind(connection, nameTemplate('service', name), { autoDelete: true }))
                        .flatMap(function(queue){
                            return Bacon
                                .fromCallback(queue.bind.bind(queue, exchange.name, name))
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
                                        instance[invocationRequest["method"]].apply(instance, _.flatten([invocationRequest["parameters"], cb]));
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
        });

    return interface;
};