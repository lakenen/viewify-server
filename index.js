'use strict';

var restify = require('restify'),
    URL = require('url'),
    viewify = require('./lib/viewify.js');


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

function isValidURL(url) {
    var parsed = URL.parse(url);
    return /https?:/.test(parsed.protocol);
}

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
        viewify.viewifyURL(url, function (err, response) {
            if (err) {
                console.error(err);
                send(res, { error: 'conversion error' }, 400);
            } else {
                send(res, response);
            }
        });
    });

    server.post('/hooks', function (req, res) {
        console.log('got a webhook notification...');
        viewify.registerWebhooks(req.body);
        res.send(200);
    });

    return server;
}

startServer();
