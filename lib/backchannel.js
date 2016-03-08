var _ = require('lodash'),
    bacon = require('baconjs');

module.exports = function(connectionProperty, backchannelId){
    return connectionProperty
        .flatMap(function(connection){ return bacon.fromCallback(connection.queue.bind(connection, backchannelId, { exclusive: true })); })
        .flatMap(function(queue){ return bacon.fromBinder(function(sink){ queue.subscribe({ exclusive: true, ack: false }, function(){ sink(_.zipObject(["message", "headers", "deliveryInfo", "messageObject"], arguments)); }); }); });

};