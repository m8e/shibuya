## Synopsis

Shibuya is a tiny library that provides easy and intuitive RPC interface that works on top of messaging systems (currently only RabbmitMQ).

## Code Example

### Defining a Shibuya shared service

All services in Shibuya are clustered. Whenever a call is made by one of the clients, a service instance is selected by the queue-provider/Shibuya's distribution logic (usually "Round-Robin"). 
Services should be designed to be business-stateless, meaning that they should not store any client-specific information exclusively, since there is it is not guaranteed that they'd be selected on subsequent client requests.

To declare a service, simply:

```
var shibuya = require('shibuya')({...configuration...});
var serviceInstance = {
    add: function(opts, cb){
        return opts["firstNumber"] + opts["secondNumber"]; 
    }
};

shibuya.registerService('service.name', serviceInstance);
```

### Calling a Shibuya shared-service
```
var shibuya = require('shibuya')({...configuration...});
shibuya.remoteCall('service.name', 'add', {
    firstNumber: 3,
    secondNumber: 2
});
```

## Motivation

Using messaging systems as a "backbone" for your system's communication provides the following benefits:

  * Easier services configuration - Every service should only know it's messaging-queue instance, that's all.
  * Load-balancing - Most queue systems provide an organic way to distribute the load of processing queue-messages.
  * Increased resilience to faults - Whenever a service drops, traffic is automatically diverted away from it.
  * Extensive debug information - All calls between the system parts can be monitored for analysis.

## Installation

```
npm install shibuya
```