var BoxView = require('box-view-queue'),
    DB = require('./db');


var view = BoxView.createClient({
    token: process.env.BOX_VIEW_API_TOKEN,
    queue: 'redis',
    queueOptions: {
        host: process.env.REDIS_HOST,
        port: process.env.REDIS_PORT
    },
    uploadQueue: {
        concurrency: 10
    }
});

var db = DB(process.env.REDIS_HOST, process.env.REDIS_PORT);

var MAX_REQUEST_DURATION = 100000; // ms
var FIVE_MINUTES = 300000; // ms

var defaultUploadParams = {
    'non_svg': false // because fuck IE 8
};
var defaultSessionParams = {
    'is_downloadable': true, // it was already downloadable, so why not?
    duration: 525900000 // 1k years
};

function getSession(url) {

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



function createDBHandler(url, callback) {

    var tid;
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
            callback(null, {
                retry: 1
            });
        }, MAX_REQUEST_DURATION);
    };

    var handler = function (err, val) {
        if (err) {
            // url does not yet exist in the db, so let's create it!
            console.log(url + ' not found; trying to convert it');

            db.set(url, { pending: true, time: Date.now() });
            getSession(url);
            startTimeout();
            return;
        }
        console.log('already have this url');

        console.log(val);
        if (typeof val === 'string') {
            console.log('WTF WHY IS THIS A STRING');
            val = JSON.parse(val);
        }

        if (val.session) {
            console.log('we have a sesion', val.session);
            callback(null, {
                session: val.session.id
            });
        } else if (val.pending) {
            console.log('this doc is still converting');
            if (Date.now() - (val.time || 0) > FIVE_MINUTES) {
                console.log('giving up on this one... retry?');
                // db.del(url);
                callback({ error: 'took too long' });
                return;
            } else {
                startTimeout();
            }
        } else {
            console.log('this url had an error', val.error);
            callback({
                error: val.error || 'unknown error...'
            });
        }
    };
    return handler;
}


module.exports = {
    viewifyURL: function (url, callback) {
        db.get(url, createDBHandler(url, callback));
    },
    registerWebhooks: function (notifications) {
        console.log('webhooks', notifications);
        view.webhooks(notifications);
    }
};
