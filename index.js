'use strict';

/*

TODO:
* (much) better error handling
* webhooks to know if a doc eventually failed (eg., after we got a valid session)
* clean this up, I mean come on


 */


var levelup = require('levelup'),
    express = require('express'),
    bodyParser = require('body-parser'),
    EventEmitter = require('eemitter'),
    hh = require('http-https'),
    validUrl = require('valid-url'),
    // https = require('https'),
    view = require('box-view').createClient(process.env.BOX_VIEW_API_TOKEN),
    app = express();

var DB = './db/urls-local';

var MAX_REQUEST_DURATION = 100;
var TEN_MINUTES = 600000;

view.documentsURL = 'http://localhost:8000/1/documents';
view.sessionsURL = 'http://localhost:8000/1/sessions';

require('http').createServer(app).listen(process.env.PORT);
console.log('listening on ' + process.env.PORT);

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

var ee = new EventEmitter();

db.on('put', function (k, v) {
    ee.emit(k, v);
});

function handleError(err, url, res) {
    console.log(err);
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
            handleError(err, url, res);
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

    db.put(url, { pending: true, time: Date.now() }, function (err) {
        if (err) {
            console.log(err);
            return;
        }
    });

    console.log('uploading: ' + url);
    view.documents.uploadURL(url, defaultUploadParams, function (err, doc) {
        console.log('finished uploading: ' + url);
        if (err) {
            handleError(err, url, res);
            return;
        }
        createSession(url, doc, res);
    });
}

function testSession(id, callback) {
    hh.get(view.sessionsURL + '/' + id + '/view', function (r) {
        if (r.statusCode === 200) {
            callback(true);
        } else {
            callback(false);
        }
    });
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
            }, MAX_REQUEST_DURATION * 1000);
            ee.on(url, tmpfn);
        } else {
            console.log('this url had an error: ' + url);
            handleError(val.error || 'unknown error...', url, res);
        }
    };
    return handler;
}

var router = express.Router();
app.use(bodyParser.json());
app.use('/', router);

router.get('/', function (req, res) {
    res.json({
        hello: 'world'
    });
});

router.get('/test', function (req, res) {
    var thing = '<a href="http://www.fws.gov/verobeach/msrppdfs/croc.pdf?'+Math.random()+'">crocodiles</a><br/>';
    res.send(
        thing + thing
        +'<a href="http://www.herpetologynotes.seh-herpetology.org/Volume7_PDFs/Dinets_HerpetologyNotes_volume7_pages3-7.pdf?'+Math.random()+'">crocodiles</a><br/>'
        +'<a href="http://nchchonors.org/wp-content/uploads/2012/04/Oswald-Brittney-Emerson-College-Paper.pdf?'+Math.random()+'">unicorns</a></br>'
        +'<a href="http://www.hazelraven.com/Unicorns.pdf?'+Math.random()+'">unicorns</a></br>'
    );
});

router.post('/doc', function (req, res) {
    var url = req.body.url;
    console.log('got url:' + url);
    if (!validUrl.isWebUri(url)) {
        res.json(400, {
            error: 'bad url'
        });
        return;
    }
    db.get(url, createDBHandler(url, res));
});

