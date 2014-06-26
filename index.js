'use strict';

var restify = require('restify'),
    hh = require('http-https'),
    URL = require('url'),
    BoxView = require('box-view-queue'),
    db = require('./lib/db');

var MAX_REQUEST_DURATION = 100000; // ms
var TWO_MINUTES = 120000; // ms

var view = BoxView.createClient({
    token: process.env.BOX_VIEW_API_TOKEN,
    queue: 'redis'
});

var defaultUploadParams = {
    'non_svg': false // because fuck IE 8
};
var defaultSessionParams = {
    'is_downloadable': true, // it was already downloadable, so why not?
    duration: 525900000 // 1k years
};

function startServer() {
    var server = restify.createServer({
        name: 'viewify'
    });
    server.listen(process.env.PORT);
    console.log('listening on ' + process.env.PORT);

    server.use(restify.queryParser());
    server.use(restify.bodyParser());
    server.use(restify.CORS());
    server.use(restify.fullResponse());
    server.use(restify.throttle({
      burst: 2,
      rate: 1,
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
        view.webhooks(req.body);
        res.send(200);
    });

    return server;
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

function sendRetry(res) {
    send(res, {
        retry: 1
    });
}

function send(res, data, code) {
    try {
        if (!code) {
            code = 200;
        }
        res.json(code, data);
    } catch (err) {
        console.error(err);
    }
}

function createDBHandler(url, req, res) {

    var tid, closed = false;
    // we have this url (maybe requested by someone else?), but it's
    // not done (or errored) yet... wait for a change in the db for this url
    var handleStatus = function (val) {
        stopTimeout();
        // call this function again
        handler(null, val);
    };

    var stopTimeout = function () {
        clearTimeout(tid);
        db.unsub(url, handleStatus);
    };

    var startTimeout = function () {
        db.sub(url, handleStatus);
        tid = setTimeout(function () {
            // tell them to retry if it's taking too long...
            sendRetry(res);
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

            db.set(url, { pending: true, time: Date.now() });

            var sessionRequest = view.viewURL(url, defaultUploadParams, defaultSessionParams);
            sessionRequest.on('error', function (err) {
                console.error('ERROR',err);
                db.set(url, { error: err });
            });
            var doc;
            sessionRequest.on('document.viewable', function (d) {
                doc = d;
            });
            sessionRequest.on('document.done', function (doc) {
                // yay document is successful
                console.log('document finished converting:' + doc.id);
                db.get(url, function (err, val) {
                    if (err) {
                        db.set(url, { error: doc });
                    } else {
                        if (!val.session) {
                            var session = view.createSession(doc.id, defaultSessionParams);
                            session.on('done', function (sess) {
                                db.set(url, {
                                    url: url,
                                    doc: doc,
                                    session: sess
                                });
                            });
                        }
                    }
                });
            });
            sessionRequest.on('done', function (sess) {
                console.log('SESSION ACQUIRED', sess);
                if (!closed) {
                    send(res, {
                        session: sess.urls.view
                    });
                    stopTimeout();
                }
                db.set(url, {
                    url: url,
                    doc: doc,
                    session: sess
                });
            });
            startTimeout();
            return;
        }
        console.log('already have this url');

        if (closed) {
            console.log('client disconnected...');
            return;
        }

        console.log(val);

        if (val.session) {
            console.log('already have a session', val.session);

            send(res, {
                session: val.session.urls.view
            });
        } else if (val.pending) {
            console.log('this url is still converting');
            if (Date.now() - (val.time || 0) > TWO_MINUTES) {
                console.log('giving up on this one...');
                send(res, {
                    error: 'the document failed to convert in a reasonable amount of time'
                }, 400);
                return;
            } else {
                startTimeout();
            }
        } else {
            console.log('this url had an error', val.error);
            send(res, {
                error: val.error || 'unknown error...'
            }, 400);
        }
    };
    return handler;
}

function isValidURL(url) {
    var parsed = URL.parse(url);
    return /https?:/.test(parsed.protocol);
}

startServer();
