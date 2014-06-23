var redis = require('redis'),
    EventEmitter = require('eemitter');

var client = redis.createClient(),
    sub = redis.createClient();
var ee = new EventEmitter();

sub.on('message', function (channel, message) {
    console.log(channel, message);
    ee.emit(channel, JSON.parse(message));
});

var subscriptions = {};
module.exports = {
    set: function (k, v, cb) {
        if (typeof v !== 'string') {
            v = JSON.stringify(v);
        }
        client.set(k, v, cb);
    },

    get: function(k, cb) {
        client.get(k, function (err, v) {
            if (!v) {
                err = 'not found';
            } else {
                v = JSON.parse(v);
            }
            cb(err, v);
        });
    },

    del: function (k) {
        client.del(k);
    },

    sub: function (channel, cb) {
        if (!subscriptions[channel]) {
            subscriptions[channel] = 1;
            console.log('subscribing to ' + channel);
            sub.subscribe(channel);
        } else {
            subscriptions[channel]++;
        }
        ee.on(channel, cb);
    },

    unsub: function (channel, cb) {
        if (!subscriptions[channel]) {
            throw new Error('nothing subscribed to ' + channel);
        }
        if (cb) {
            subscriptions[channel]--;
        } else {
            // unsubscribe all
            subscriptions[channel] = 0;
        }
        if (subscriptions[channel] === 0) {
            console.log('unsubscribing from ' + channel);
            sub.unsubscribe(channel);
        }
        ee.off(channel, cb);
    },

    pub: function (channel, message) {
        console.log(channel, message);
        client.publish(channel, JSON.stringify(message));
    }
};
