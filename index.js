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
};

util.inherits(Constructor, EventEmitter);

_.assign(Constructor.prototype, {
    call: notImplemented,
    register: notImplemented
});

module.exports = Constructor;

