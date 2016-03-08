"use strict";

var _ = require('lodash'),
    util = require('util'),
    uuid = require('node-uuid'),
    amqp = require('amqp'),
    bacon = require('baconjs'),
    EventEmitter = require('events'),
    backchannelGenerator = require('./lib/backchannel');

var notImplemented = function(){ throw(new Error('Not Implemented')); }

var Constructor = function(config){

    var backChannelId = _.template('shibuya-backchannel-<%-uuid%>')({ uuid: uuid.v4() }),
        exchangeId = ["shibuya", config["exchange_id"] || "exchange"].join('-');

    // Create generic connection property
    var connectionProperty = bacon.fromCallback(function(cb){
        (function(con){ con.on('ready', _.once(_.partial(cb, con))); })(amqp.createConnection(config["connection"], { reconnect: true }));
    }).toProperty();

    var exchangeProperty = connectionProperty.flatMap(function(connection){
        return bacon.fromCallback(connection.exchange.bind(connection, exchangeId, { type: "topic", autoDelete: true, durable: false }));
    }).toProperty().log('Exchange');

    // Create backchannel
    var backchannelStream = backchannelGenerator(connectionProperty, backChannelId);

    // Create calling pipeline
    bacon
        .fromBinder(function(sink){
            this.call = function(serviceIdentifier, options, callback){
                sink({
                    serviceIdentifier: serviceIdentifier,
                    options: options,
                    callback: callback
                });
            };
        }.bind(this))
        .combine(connectionProperty, function(call, connection){
            return _.assign(call, { connection: connection, callId: uuid.v4() })
        })
        .flatMap(function(call){

        })
        //.log();
};

util.inherits(Constructor, EventEmitter);

_.assign(Constructor.prototype, {
    call: notImplemented,
    register: notImplemented
});

module.exports = Constructor;

