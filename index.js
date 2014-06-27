'use strict';

var restify = require('restify'),
    hh = require('http-https'),
    URL = require('url'),
    BoxView = require('box-view-queue'),
    db = require('./lib/db')(process.env.REDIS_HOST, process.env.REDIS_PORT);

var MAX_REQUEST_DURATION = 100000; // ms
var TWO_MINUTES = 120000; // ms

var view = BoxView.createClient({
    token: process.env.BOX_VIEW_API_TOKEN,
    queue: 'redis',
    queueOptions: {
        host: process.env.REDIS_HOST,
        port: process.env.REDIS_PORT
    }
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

// function verifySession(assetURL, callback) {
//     console.log('testing ' + assetURL + 'info.json');
//     var req = hh.get(assetURL + 'info.json', function (r) {
//         if (r.statusCode === 200) {
//             callback(true);
//         } else {
//             callback(false);
//         }
//     });
//     req.end();
// }

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

function sendSession(res, session) {
    send(res, {
        session: session.urls.view
    });
}

function getSession(url, res) {

    var sessionSent = false;
    var sessionRequest = view.viewURL(url, defaultUploadParams, defaultSessionParams);

    function handleSession(session, doc) {
        console.log('SESSION ACQUIRED', session);
        db.set(url, {
            url: url,
            doc: doc,
            session: session
        });
        sessionSent = true;
    }

    sessionRequest.on('error', function (err) {
        console.error('ERROR', err);
        db.set(url, { error: err });
        sessionRequest.off();
    });
    sessionRequest.on('done', function (session, doc) {
        if (!sessionSent) {
            handleSession(session, doc);
        }
    });
    sessionRequest.on('document.done', function (doc) {
        // yay document is successful
        console.log('document finished converting:' + doc.id);
        sessionRequest.off();

        if (!sessionSent) {
            console.log('requesting session because we haven\'t gotten one yet');
            var session = view.createSession(doc.id, defaultSessionParams);
            session.one('done', function (sess) {
                handleSession(sess, doc);
            });
        }
    });
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
            getSession(url, res);
            startTimeout();
            return;
        }
        console.log('already have this url');

        if (closed) {
            console.log('client disconnected...');
            return;
        }

        console.log(val);
        if (typeof val === 'string') {
            console.log('WTF WHY IS THIS A STRING');
            val = JSON.parse(val);
        }

        if (val.session) {
            console.log('we have a sesion', val.session);
            sendSession(res, val.session);
        } else if (val.pending) {
            console.log('this doc is still converting');
            if (Date.now() - (val.time || 0) > TWO_MINUTES) {
                console.log('giving up on this one... retry?');
                db.del(url);
                sendRetry(res);
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
