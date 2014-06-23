var queue = require('bull'),
    view = require('box-view').createClient(process.env.BOX_VIEW_API_TOKEN),
    db = require('../lib/db');

view.documentsURL = process.env.BOX_VIEW_DOCUMENTS_URL || view.documentsURL;

var uploadQueue = queue('uploads', 6379, '127.0.0.1');
var sessionQueue = queue('sessions', 6379, '127.0.0.1');
uploadQueue.process(processJob);

var defaultUploadParams = {
    'non_svg': false // because fuck IE 8
};

function updateURLStatus(url, data) {
    db.set(url, data);
    db.pub(url, data);
}

function processJob(job, done) {
    var url = job.data.url;
    console.log('uploading: ' + url);

    updateURLStatus(url, {
        pending: true,
        time: Date.now()
    });
    view.documents.uploadURL(url, defaultUploadParams, function (err, response) {
        var dbkey, id;
        console.log('finished uploading: ' + url, response);
        if (err) {
            console.log('there was an error...');
            response = err.response;
            if (response && response.statusCode === 429) {
                var retryAfter = response.headers['retry-after'];
                console.log('got throttled... retrying in ' + retryAfter + 's');
                setTimeout(function () {
                    processJob(job, done);
                }, retryAfter * 1000);
            } else {
                // upload failed for some reason... we're done here
                updateURLStatus(url, {
                    error: err.error
                });
                done(err.error);
            }
            return;
        }
        id = response.id;
        dbkey = 'doc-' + id;
        db.set(dbkey, {
            doc: response,
            url: url
        });
        console.log('waiting for a webhook...');
        db.sub('webhook', function handleWebhook(message) {
            console.log('got a webhook for  ' + message.doc + ':' + message.type);
            if (message.doc === id) {
                // create session job
                switch (message.type) {
                    case 'document.viewable':
                        sessionQueue.add({ url: url, doc: id });
                        break;
                    case 'document.error':
                    case 'document.done':
                        // request the full status of a document
                        view.documents.get(id, function (err, doc) {
                            var error = doc ? null : err.error;
                            if (doc && doc.status === 'error') {
                                error = 'conversion error';
                            }
                            var metadata = {
                                url: url,
                                doc: doc,
                                error: error
                            };
                            db.set(dbkey, metadata);
                            if (error) {
                                updateURLStatus(url, metadata);
                            }
                        });
                        db.unsub('webhook', handleWebhook);
                        done();
                        break;
                }
            }
        });
    });
}
