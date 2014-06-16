'use strict';

/*

TODO:
* (much) better error handling
* webhooks to know if a doc eventually failed (eg., after we got a valid session)
* clean this up, I mean come on


 */


var levelup = require('levelup'),
    async = require('async'),
    restify = require('restify'),
    amqp = require('amqp'),
    EventEmitter = require('eemitter'),
    hh = require('http-https'),
    URL = require('url'),
    // https = require('https'),
    view = require('box-view').createClient(process.env.BOX_VIEW_API_TOKEN);

var DB = './db/urls-local';
var DOC_DB = './db/docs-local';

var MAX_REQUEST_DURATION = 100000; // ms
var TEN_MINUTES = 600000; // ms

var ERR_CONVERSION = 'There was an error converting the document.';
var ERR_SESSION = 'There was an error creating a viewing session. This probably means the document failed to convert.';


view.documentsURL = 'http://localhost:8000/1/documents';
view.sessionsURL = 'http://localhost:8000/1/sessions';

var defaultUploadParams = {
        'non_svg': false // because fuck IE 8
    },
    defaultSessionParams = {
        'is_downloadable': true, // it was already downloadable, so why not?
        duration: 525900000 // 1k years
    };

var db = levelup(DB, {
    valueEncoding: 'json'
});
var docDB = levelup(DOC_DB, {
    valueEncoding: 'json'
});

var ee = new EventEmitter();

var connection = amqp.createConnection();
var queue = connection.queue('conversions', function (q) {
    q.subscribe(function (message, headers, deliveryInfo, messageObject) {
        console.log('Got a message with routing key ' + deliveryInfo.routingKey);
        messageObject.acknowledge();
    });
});

db.on('put', function (k, v) {
    ee.emit(k, v);
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
        connection.publish('url', {foo: 'blah'});
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
        db.get(url, createDBHandler(url, res));
    });

    server.post('/hooks', function (req, res) {
        console.log('got a webhook notification...');
        var notifications = req.body;
        var erroredDocIds = notifications.filter(function (n) {
            return n.type === 'document.error';
        }).map(function (n) {
            return n.data.id;
        });
        if (erroredDocIds.length) {
            console.log('got ' + erroredDocIds.length + ' errors');
            console.log(erroredDocIds)
            async.map(erroredDocIds, registerConversionError, function (err, result) {
                console.log('finished registering errors');
            });
        }
        res.send(200);
    });

    return server;
}

function registerConversionError(id, callback) {
    docDB.get(id, function (err, doc) {
        if (err) {
            console.log('could not find a doc with id ' + id);
            callback();
        } else {
            db.get(doc.url, function (err, val) {
                if (err) {
                    val = { error: ERR_CONVERSION, doc: id };
                } else {
                    val.error = ERR_CONVERSION;
                    delete val.session;
                }
                db.put(doc.url, val, function (err) {
                    if (err) {
                        console.log('error updating doc...', val);
                    }
                    callback();
                });
            });
        }
    });
}

function handleError(err, url, res) {
    console.log('ERROR', err);
    var metadata = {
        error: err
    };
    db.put(url, metadata, function (err) {
        if (err) {
            console.log(err);
            return;
        }
    });
    res.json(400, metadata);
}

function createSession(url, doc, res) {
    console.log('creating session: ' + url, doc);
    view.sessions.create(doc.id, defaultSessionParams, function (err, session) {
        var metadata;
        console.log('finished creating session: ' + url);

        if (err) {
            handleError(ERR_SESSION, url, res);
            return;
        }


        console.log('got a valid session!', session);
        metadata = {
            url: url,
            doc: doc.id,
            session: session.id
        };

        // we got a doc! let's update the db...
        db.put(url, metadata, function (err) {
            if (err) {
                console.log(err);
                return;
            }
        });

        res.json({
            session: metadata.session
        });
    });
}

function uploadDoc(url, res) {

    console.log('uploading: ' + url);
    view.documents.uploadURL(url, defaultUploadParams, function (err, response) {
        console.log('finished uploading: ' + url);
        if (err) {
            var retryAfter = response.headers['retry-after'];
            if (response.statusCode === 429) {
                console.log('got throttled... retrying in ' + retryAfter + 's');
                setTimeout(function () {
                    uploadDoc(url, res);
                }, retryAfter * 1000);
            } else {
                handleError(err, url, res);
            }
            return;
        }
        docDB.put(response.id, {
            doc: response,
            url: url
        });
        createSession(url, response, res);
    });
}

function testSession(id, callback) {
    var options = URL.parse(view.sessionsURL + '/' + id + '/view');
    options.method = 'HEAD';
    var req = hh.request(options, function (r) {
        if (r.statusCode === 200) {
            callback(true);
        } else {
            callback(false);
        }
    });
    req.end();
}

function handleSession(id, url, res) {
    console.log('got a session ' +id +', make sure it works...');
    testSession(id, function (valid) {
        if (valid) {
            console.log('session is valid for '+ url + '... go get it!');
            res.json({
                session: id
            });
        } else {
            // session is bad... delete it from the db, and tell the client to retry?
            console.log('bad session for '+ url + '... retry?');
            db.del(url);
            res.json({
                retry: 1
            });
        }
    });
}

function createDBHandler(url, res) {
    var handler = function (err, val) {
        if (err) {
            // url does not yet exist in the db, so let's create it!
            console.log('could not find '+ url + ' in the db, so trying to convert it');

            db.put(url, { pending: true, time: Date.now() }, function (err) {
                if (err) {
                    console.log(err);
                    return;
                }
            });

            uploadDoc(url, res);
            return;
        }
        console.log('already have this url: ' + url);

        if (val.session) {
            console.log('already have a session for url: ' + url);
            handleSession(val.session, url, res);
        } else if (val.pending) {
            console.log('this url is still converting?: ' + url);
            if (Date.now() - (val.time || 0) > TEN_MINUTES) {
                console.log('giving up on this one...');
                handleError('the document failed to convert in a reasonable amount of time', url, res);
                return;
            }
            // we have this url (maybe requested by someone else?), but it's
            // not done (or errored) yet... wait for a change in the db for this url
            var tmpfn = function (val) {
                clearTimeout(tid);
                handler(null, val);
                ee.off(url, tmpfn);
            };
            var tid = setTimeout(function () {
                // tell them to retry if it's taking too long...
                res.json({
                    retry: 1
                });
                ee.off(url, tmpfn);
            }, MAX_REQUEST_DURATION);
            ee.on(url, tmpfn);
        } else {
            console.log('this url had an error: ' + url);
            handleError(val.error || 'unknown error...', url, res);
        }
    };
    return handler;
}

function isValidURL(url) {
    var parsed = URL.parse(url);
    return /https?:/.test(parsed.protocol);
}

startServer();
