'use strict';

var restify = require('restify'),
    URL = require('url'),
    DB = require('./lib/db.js'),
    email = require('email-validation'),
    viewify = require('./lib/viewify.js');

var API_PREFIX = '/api/1/';

var db = DB(process.env.REDIS_HOST, process.env.REDIS_PORT);

function randomInt (low, high) {
    return Math.floor(Math.random() * (high - low) + low);
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

    server.get(API_PREFIX, function (req, res) {
        //createJob(req.params.url);
        res.json({
            hello: 'world'
        });
    });

    server.post(API_PREFIX + '/docs', function (req, res) {
        var url = req.body.url;
        // console.log('got url:' + url);
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

    server.post(API_PREFIX + '/hooks', function (req, res) {
        // console.log('got a webhook notification...');
        viewify.registerWebhooks(req.body);
        res.send(200);
    });

    server.post(API_PREFIX + '/auth', function (req, res) {
        var submittedEmail = req.body.email;
        if (!email.valid(submittedEmail)) {
            res.json(422, {error: 'invalid email'});
        } else {
            var randomKey, existingKey;
            while (true) {
                randomKey = randomInt(10000000,99999999);
                existingKey = db.get('User:' + randomKey);
                if (!existingKey) {
                    break;
                }
            }
            db.set('User:' + randomKey, submittedEmail);
            res.json({id: randomKey});
        }
    });
}

startServer();
