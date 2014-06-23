'use strict';

var restify = require('restify'),
    hh = require('http-https'),
    URL = require('url'),
    queue = require('bull'),
    db = require('./lib/db');

var MAX_REQUEST_DURATION = 100000; // ms
var TEN_MINUTES = 600000; // ms

var uploadQueue = queue('uploads', 6379, '127.0.0.1');
uploadQueue.empty().then(function () {
    console.log('emptied the queue');
});

function startServer() {
    var server = restify.createServer({
        name: 'viewify'
    });
    server.listen(process.env.PORT);
    console.log('listening on ' + process.env.PORT);

    server.use(restify.queryParser());
    server.use(restify.bodyParser());
    server.use(restify.throttle({
      burst: 20,
      rate: 10,
      ip: true,
      overrides: {
        '127.0.0.1': {
          rate: 0,        // unlimited
          burst: 0
        }
      }
    }));

    server.get('/', function (req, res) {
        //createJob(req.params.url);
        res.json({
            hello: 'world'
        });
    });

    server.post('/docs', function (req, res) {
        var url = req.body.url;
        console.log('got url:' + url);
        if (!isValidURL(url)) {
            res.json(400, {
                error: 'bad url'
            });
            return;
        }
        db.get(url, createDBHandler(url, req, res));
    });

    server.post('/hooks', function (req, res) {
        console.log('got a webhook notification...');
        var notifications = req.body;
        if (notifications.length) {
            console.log('got ' + notifications.length + ' webhooks');
            notifications.forEach(function (n) {
                console.log('publishing webhook for ', n.data.id, n.type);
                db.pub('webhook', {
                    type: n.type,
                    doc: n.data.id
                });
            });
        }
        res.send(200);
    });

    return server;
}

function createJob(url) {
    uploadQueue.add({ url: url });
    uploadQueue.count().then(function (n) {
        console.log('created job... queue length: ' + n);
    });
}

function verifySession(assetURL, callback) {
    console.log('testing ' + assetURL + 'info.json');
    var req = hh.get(assetURL + 'info.json', function (r) {
        if (r.statusCode === 200) {
            callback(true);
        } else {
            callback(false);
        }
    });
    req.end();
}

function handleSession(sess, url, res) {
    console.log('got a session ' +sess.id +', make sure it works...');
    verifySession(sess.urls.assets, function (valid) {
        if (valid) {
            console.log('session is valid... go get it!');
            res.json({
                session: sess.urls.view
            });
        } else {
            // session is bad... delete it from the db, and tell the client to retry?
            console.log('bad session... retry?');
            db.del(url);
            res.json({
                retry: 1
            });
        }
    });
}

function createDBHandler(url, req, res) {

    var tid, closed = false;
    // we have this url (maybe requested by someone else?), but it's
    // not done (or errored) yet... wait for a change in the db for this url
    var handleStatus = function (val) {
        clearTimeout(tid);
        // call this function again
        handler(null, val);
        db.unsub(url, handleStatus);
    };

    var startTimeout = function () {
        db.sub(url, handleStatus);
        tid = setTimeout(function () {
            // tell them to retry if it's taking too long...
            handleStatus({
                retry: 1
            });
        }, MAX_REQUEST_DURATION);
    };

    req.on('end', function () {
        closed = true;
    });

    req.on('close', function () {
        closed = true;
    });

    var handler = function (err, val) {
        if (err) {
            // url does not yet exist in the db, so let's create it!
            console.log(url + ' not found; trying to convert it');

            // db.set(url, { pending: true, time: Date.now() }, function (err) {
            //     if (err) {
            //         console.log(err);
            //         return;
            //     }
            // });

            startTimeout();
            createJob(url);
            return;
        }
        console.log('already have this url');

        if (closed) {
            console.log('client disconnected...')
            return;
        }

        if (val.session) {
            console.log('already have a session', val.session);
            handleSession(val.session, url, res);
        } else if (val.pending) {
            console.log('this url is still converting');
            if (Date.now() - (val.time || 0) > TEN_MINUTES) {
                console.log('giving up on this one...');
                res.json(400, {
                    error: 'the document failed to convert in a reasonable amount of time'
                });
                return;
            } else {
                startTimeout();
            }
        } else {
            console.log('this url had an error', val.error);
            res.json(400, {
                error: val.error || 'unknown error...'
            });
        }
    };
    return handler;
}

function isValidURL(url) {
    var parsed = URL.parse(url);
    return /https?:/.test(parsed.protocol);
}

startServer();
