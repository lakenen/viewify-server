var queue = require('bull'),
    view = require('box-view').createClient(process.env.BOX_VIEW_API_TOKEN),
    db = require('../lib/db');

view.documentsURL = process.env.BOX_VIEW_DOCUMENTS_URL;
view.sessionsURL = process.env.BOX_VIEW_SESSIONS_URL;

var sessionQueue = queue('sessions', 6379, '127.0.0.1');
sessionQueue.process(processJob);

var defaultSessionParams = {
    'is_downloadable': true, // it was already downloadable, so why not?
    duration: 525900000 // 1k years
};

function updateURLStatus(url, data) {
    db.set(url, data);
    db.pub(url, data);
}

function processJob(job, done) {
    var url = job.data.url,
        id = job.data.doc;
    console.log('creating session for ', url);
    view.sessions.create(id, defaultSessionParams, function (err, session) {
        if (err) {
            console.log('error creating session', err.error);
            updateURLStatus(url, {
                error: err.error || err
            });
            done(err);
            return;
        }

        console.log('got a valid session!', session);

        // we got a doc! let's update the db...
        updateURLStatus(url, {
            url: url,
            doc: id,
            session: session
        });
        done();
    });
}
