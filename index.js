'use strict';

/*

TODO:
* (much) better error handling
* webhooks to know if a doc eventually failed (eg., after we got a valid session)
* clean this up, I mean come on


 */


var async = require('async'),
    restify = require('restify'),
    EventEmitter = require('eemitter'),
    hh = require('http-https'),
    URL = require('url'),
    redis = require('redis'),
    queue = require('bull'),
    db = redis.createClient(),
    view = require('box-view').createClient(process.env.BOX_VIEW_API_TOKEN);

var MAX_REQUEST_DURATION = 100000; // ms
var TEN_MINUTES = 600000; // ms

var ERR_CONVERSION = 'There was an error converting the document.';
var ERR_SESSION = 'There was an error creating a viewing session. This probably means the document failed to convert.';

view.documentsURL = process.env.BOX_VIEW_DOCUMENTS_URL;
view.sessionsURL = process.env.BOX_VIEW_SESSIONS_URL;

var defaultUploadParams = {
        'non_svg': false // because fuck IE 8
    },
    defaultSessionParams = {
        'is_downloadable': true, // it was already downloadable, so why not?
        duration: 525900000 // 1k years
    };

var ee = new EventEmitter();

var urlQueue = queue('urls', 6379, '127.0.0.1');

function emitUpdate(url) {
    get(url, function (err, val) {
        if (!err) {
            ee.emit(url, val);
        }
    });
}
urlQueue.process(function (job, done) {
    console.log('got a job:', job.data);
    var url = job.data.url;
    uploadDoc(url, function (err, metadata) {
        if (err) {
            console.log('error uploading doc', err);
        }
        console.log('finished... session is available... sending update');
        emitUpdate(url);
        ee.one('webhook-' + metadata.doc, function (err) {
            console.log('got a webhook; pulling next doc from the queue...');
            job.remove();
            done(err);
        });
        setTimeout(function () {
            done('took too long to convert');
        }, 60000);
    });
});

function set(k, v, cb) {
    if (typeof v !== 'string') {
        v = JSON.stringify(v);
    }
    db.set(k, v, cb);
}

function get(k, cb) {
    db.get(k, function (err, v) {
        if (!v) {
            err = 'not found';
        } else {
            v = JSON.parse(v);
        }
        cb(err, v);
    });
}

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
        get(url, createDBHandler(url, res));
    });

    server.post('/hooks', function (req, res) {
        console.log('got a webhook notification...');
        var notifications = req.body;
        if (notifications.length) {
            console.log('got ' + notifications.length + ' webhooks');
            async.map(notifications, handleWebhook, function (err, result) {
                console.log('finished handling webhooks');
            });
        }
        res.send(200);
    });

    return server;
}

function createJob(url) {
    urlQueue.add({ url: url });
}

function handleWebhook(hook, callback) {
    if (hook.type === 'document.error') {
        console.log('document failed ' + hook.data.id);
        registerConversionError(hook.data.id, callback);
        ee.emit('webhook-' + hook.data.id, 'conversion error');
    } else if (hook.type === 'document.done') {
        console.log('document is finished ' + hook.data.id);
        ee.emit('webhook-' + hook.data.id);
        callback();
    }
}

function registerConversionError(id, callback) {
    get('doc-' + id, function (err, doc) {
        if (err) {
            console.log('could not find a doc with id ' + id);
            callback();
        } else {
            get(doc.url, function (err, val) {
                if (err) {
                    val = { error: ERR_CONVERSION, doc: id };
                } else {
                    val.error = ERR_CONVERSION;
                    delete val.session;
                }
                set(doc.url, val, function (err) {
                    if (err) {
                        console.log('error updating doc...', val);
                    }
                    callback();
                });
            });
        }
    });
}

function handleError(err, url) {
    console.log('ERROR', err);
    var metadata = {
        error: err
    };
    set(url, metadata);
}

function createSession(url, doc, done) {
    console.log('creating session: ' + url, doc);
    view.sessions.create(doc.id, defaultSessionParams, function (err, session) {
        var metadata;
        console.log('finished creating session: ' + url);

        if (err) {
            handleError(ERR_SESSION, url);
            done(err);
            return;
        }


        console.log('got a valid session!', session);
        metadata = {
            url: url,
            doc: doc.id,
            session: session.id
        };

        // we got a doc! let's update the db...
        set(url, metadata, function (err) {
            if (err) {
                done(err);
                return;
            }
            done(null, metadata);
        });
    });
}

function uploadDoc(url, done) {
    console.log('uploading: ' + url);
    view.documents.uploadURL(url, defaultUploadParams, function (err, response) {
        console.log('finished uploading: ' + url);
        if (err) {
            response = err.response;
            if (response && response.statusCode === 429) {
                var retryAfter = response.headers['retry-after'];
                console.log('got throttled... retrying in ' + retryAfter + 's');
                setTimeout(function () {
                    uploadDoc(url, done);
                }, retryAfter * 1000);
            } else {
                handleError(err, url);
                done(err);
            }
            return;
        }
        set('doc-' + response.id, {
            doc: response,
            url: url
        });
        createSession(url, response, done);
    });
}

/*function uploadDoc(url, res) {

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
}*/
/*
function readResponse(response, callback) {
    var body = '';
    response.on('data', function (d) {
        body += d.toString();
    });
    response.on('end', function () {
        callback(body);
    });
    response.on('error', callback);
}*/

function verifySession(id, callback) {
    var req = hh.get(view.sessionsURL + '/' + id + '/assets/info.json', function (r) {
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
    verifySession(id, function (valid) {
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

    var tid;
    // we have this url (maybe requested by someone else?), but it's
    // not done (or errored) yet... wait for a change in the db for this url
    var handleResult = function (val) {
        clearTimeout(tid);
        // call this function again
        handler(null, val);
    };

    var startTimeout = function () {
        ee.one(url, handleResult);
        tid = setTimeout(function () {
            // tell them to retry if it's taking too long...
            res.json({
                retry: 1
            });
            ee.off(url, handleResult);
        }, MAX_REQUEST_DURATION);
    };

    var handler = function (err, val) {
        if (err) {
            // url does not yet exist in the db, so let's create it!
            console.log('could not find '+ url + ' in the db, so trying to convert it');

            set(url, { pending: true, time: Date.now() }, function (err) {
                if (err) {
                    console.log(err);
                    return;
                }
            });

            startTimeout();
            createJob(url);
            return;
        }
        console.log('already have this url: ' + url);

        if (val.session) {
            console.log('already have a session for url: ' + url);
            handleSession(val.session, url, res);
        } else if (val.pending) {
            console.log('this url is still converting?: ' + url);
            startTimeout();
            if (Date.now() - (val.time || 0) > TEN_MINUTES) {
                console.log('giving up on this one...');
                handleError('the document failed to convert in a reasonable amount of time', url);
                return;
            }
        } else {
            console.log('this url had an error: ' + url);
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
