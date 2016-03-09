"use strict";

var _ = require('lodash'),
    util = require('util'),
    uuid = require('node-uuid'),
    amqp = require('amqp'),
    bacon = require('baconjs'),
    EventEmitter = require('events');

var CALL_TIMEOUT = 1000,
    DELIVERY_MODE_NON_PERSISTENT = 1;

var notImplemented = function(){ throw(new Error('Not Implemented')); };

var Constructor = function(config){

    var backchannelId = _.template('shibuya-backchannel-<%-uuid%>')({ uuid: uuid.v4() }),
        exchangeId = ["shibuya", config["exchange_id"] || "exchange"].join('-');

    // Create generic connection property
    var connectionProperty = bacon.fromCallback(function(cb){
        (function(con){ con.on('ready', _.once(_.partial(cb, con))); })(amqp.createConnection(config["connection"], { reconnect: true }));
    }).toProperty();

    var exchangeProperty = connectionProperty.flatMap(function(connection){
        return bacon.fromCallback(connection.exchange.bind(connection, exchangeId, { type: "topic", autoDelete: true, durable: false }));
    }).toProperty();

    // Create backchannel
    var backchannelStream =  connectionProperty
        .flatMap(function(connection){ return bacon.fromCallback(connection.queue.bind(connection, backchannelId, { exclusive: true })); })
        .flatMap(function(queue){ return bacon.fromBinder(function(sink){ queue.subscribe({ exclusive: true, ack: false }, function(){ sink(_.zipObject(["message", "headers", "deliveryInfo", "messageObject"], arguments)); }); }); });

    // Create calling pipeline
    bacon
        .fromBinder(function(sink){
            this.call = function(serviceIdentifier, options, callback){
                sink({
                    serviceIdentifier: serviceIdentifier,
                    options: options || {},
                    callback: callback
                });
            };
        }.bind(this))
        .combine(exchangeProperty, function(call, exchange){ return _.assign(call, { exchange: exchange, callId: uuid.v4() }); })
        .flatMap(function(call){
            // Send the message out
            call["exchange"].publish(["shibuya", call["serviceIdentifier"]].join('-'), call["options"], {
                contentType: "application/json",
                deliveryMode: DELIVERY_MODE_NON_PERSISTENT,
                replyTo: backchannelId,
                correlationId: call["callId"]
            });

            return backchannelStream
                .filter(function(message){ return message["deliveryInfo"]["correlationId"] === call["callId"] })
                .map(function(message){ return { status: "ok", message: message, call: call }; })
                .merge(bacon.later(CALL_TIMEOUT, { status: "timeout", call: call }))
                .take(1)
        })
        .onValue(function(res){
            // Return a response to caller
            res["call"]["callback"]((res["status"] !== "ok" && new Error(res["status"])) || null, res["message"]);
        });


    // Create service pipeline

    bacon.combineTemplate({
        exchange: exchangeProperty,
        connection: connectionProperty,
        registration: bacon.fromBinder(function(sink){
            this.register = function(serviceIdentifier, instance){
                sink({
                    serviceIdentifier: serviceIdentifier,
                    instance: instance
                });
            };
        }.bind(this))
    }).flatMap(function(request){
        return bacon.combineTemplate({
            queue: bacon.fromCallback(
                request["connection"].queue.bind(
                    request["connection"],
                    ["shibuya", "service", request["registration"]["serviceIdentifier"]].join('-'), { ack: false }
                )
            ).map(function(queue){
                // Binds the service queue to topic "shibuya-serviceIdentifier"
                queue.bind(request["exchange"], ["shibuya", ["shibuya", request["registration"]["serviceIdentifier"]].join('-')]);
                return queue;
            })
        });
    }).log();

    /*bacon
        .fromBinder(function(sink){
            ;
        }.bind(this))
        .combine(connectionProperty, function(serviceRequest, connection){
            return _.assign(serviceRequest, { connection: connection })
        })
        .flatMap(function(serviceRequest){
            // Create/Join service queue
            return bacon.fromCallback(
                serviceRequest["connection"].queue.bind(
                    serviceRequest["connection"],
                    ["shibuya", "service", serviceRequest["serviceIdentifier"]].join('-'),
                    { exclusive: false }
                )).flatMap(function(queue){
                    bacon.fromBinder(function(sink){
                        queue.subscribe({ ack: false }, function(){ sink(_.zipObject(["message", "headers", "deliveryInfo", "messageObject"], arguments)); })
                    });
                });
        }).log('Service Queue Created');*/
};

util.inherits(Constructor, EventEmitter);

_.assign(Constructor.prototype, {
    call: notImplemented,
    register: notImplemented
});

module.exports = Constructor;

