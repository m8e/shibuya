var _ = require('lodash'),
    bacon = require('baconjs'),
    amqp = require('amqp'),
    uuid = require('node-uuid');

var AMQP_RECONNECT_STRATEGY_LINEAR = "linear",
    AMQP_RECONNECT_BACKOFF_TIME = 1000,
    AMQP_EXCHANGE_TYPE_TOPIC = "topic",
    AMQP_EXCHANGE_DELIVERY_MODE_NON_PERSISTENT = 1,
    RESPONSE_TIMEOUT = 3000;

module.exports = function(configuration){
    var CLIENT_ID = uuid.v4();

    var sharedId = configuration["shared_id"] || "SHIBUYA";

    /** Shared Resources **/
    // Establish local connection
    var connectionProperty = (function(connection){
        return bacon
            .fromEvent(connection, 'ready')
            .map(connection)
            .take(1);
    })(amqp.createConnection(
        configuration["connection"],
        {
            reconnect: true,
            reconnectBackoffStrategy: AMQP_RECONNECT_STRATEGY_LINEAR,
            reconnectBackoffTime: AMQP_RECONNECT_BACKOFF_TIME
        }
    ));

    // Define/Connect to system-wide exchange (should be shared across all Shibuya's system components)
    var exchangeProperty = connectionProperty
        .flatMap(function(connection){
            return bacon.fromCallback(connection.exchange.bind(
                connection,
                [sharedId, "exchange"].join('|'),
                {
                    type: AMQP_EXCHANGE_TYPE_TOPIC,
                    durable: false
                }
            ));
        });

    // Shared feedback channel
    var feedbackStream = connectionProperty
        .flatMap(function(connection){ return bacon.fromCallback(connection.queue.bind(connection, [sharedId, "feedback", CLIENT_ID].join('|'), { exclusive: true })); })
        .doAction(function(queue){
            queue.bind([sharedId, "exchange"].join('|'), [sharedId, "feedback", CLIENT_ID].join('|'));
        })
        .flatMap(function(queue){
            return bacon.fromBinder(function(sink){
                queue.subscribe(
                    { ack: false },
                    function(message, headers, deliveryInfo, messageObject){
                        sink({
                            message: message,
                            headers: headers,
                            deliveryInfo: deliveryInfo,
                            messageObject: messageObject
                        });
                    }
                );
            });
        });


    feedbackStream.onValue(_.noop);

    var interface = {};

    //** Service Declaration **/
    bacon
        .fromBinder(function(sink){
            interface.register = function(serviceName, instance){ sink({ serviceName: serviceName, instance: instance }); };
            return function(){ interface.register = function(){ throw(new Error('Method no longer available')); } };
        })
        .flatMap(function(registrationRequest){
            var serviceIdentifier = [sharedId, "service", registrationRequest["serviceName"]].join('|'),
                instance = registrationRequest["instance"];
            return connectionProperty
                .flatMap(function(connection){
                    return bacon.fromCallback(connection.queue.bind(
                        connection,
                        serviceIdentifier,
                        {}
                    ));
                })
                .combine(exchangeProperty, function(queue, exchange){
                    queue.bind(exchange, serviceIdentifier);
                    return queue;
                })
                .flatMap(function(queue){
                    return bacon.fromBinder(function(sink){ return queue.subscribe({ ack: false }, function(message, headers, deliveryInfo, messageObject){
                            sink({
                                message: message,
                                headers: headers,
                                deliveryInfo: deliveryInfo,
                                messageObject: messageObject
                            });
                        });
                    });
                })
                .flatMap(function(remoteCall){

                    return bacon
                        .fromNodeCallback(instance[remoteCall.message["method"]].bind(instance, remoteCall.message["options"]))
                        .map(function(data){
                            return {
                                status: "ok",
                                payload: data
                            };
                        })
                        .mapError(function(error){
                            return {
                                status: "error",
                                payload: error
                            };
                        })
                        .map(function(obj){
                            return _.assign(obj, {
                                correlation_id: remoteCall.deliveryInfo["correlationId"],
                                reply_to: remoteCall.deliveryInfo["replyTo"]
                            });
                        })
                });
        })
        .combine(exchangeProperty, function(response, exchange){
            return { response: response, exchange: exchange };
        })
        .onValue(function(obj){
            var exchange = obj["exchange"],
                response = obj["response"];

            exchange.publish(response["reply_to"], _.pick(response, 'status', 'payload'), {
                deliveryMode: AMQP_EXCHANGE_DELIVERY_MODE_NON_PERSISTENT,
                correlationId: response["correlation_id"]
            });
        });

    /** Consumer **/
    bacon.fromBinder(function(sink){
        interface.remoteCall = function(serviceName, methodName, options, callback){
            sink({
                serviceName: serviceName,
                methodName: methodName,
                options: options,
                callback: callback || _.noop,
                id: uuid.v4()
            });
        };
    })
    .combine(exchangeProperty, function(call, exchange){
        return _.assign(call, { exchange: exchange });
    })
    .flatMap(function(call) {
        call.exchange.publish(
            [sharedId, "service", call.serviceName].join('|'),
            {
                method: call.methodName,
                options: call.options
            },
            {
                replyTo: [sharedId, "feedback", CLIENT_ID].join('|'),
                correlationId: call.id
            }
        );

        return feedbackStream
            .filter(function(o){ return o.deliveryInfo["correlationId"] === call.id; })
            .map('.message')
            .merge(bacon.later(RESPONSE_TIMEOUT, { status: "error", payload: new Error('Failed to obtain response in a timely fashion') }))
            .take(1)
            .map(function(o){ return _.assign(o, { callback: call["callback"] }); });

    })
    .onValue(function(message){
        message.callback((message.status !== "ok" || null) && new Error(message.payload), message.payload);
    });

    return interface;
};