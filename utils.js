var mysql = require('mysql'),
    _ = require('lodash'),
    url = require('url');

var utils = module.exports = {};

utils.parseUrl = (config) => {
    if (!_.isString(config.url)) {
        return config;
    }

    var obj = url.parse(config.url);

    config.host = obj.hostname || config.host;
    config.port = obj.port || config.port;

    if (_.isString(obj.pathname)) {
        config.database = obj.pathname.split('/')[1] || config.database;
    }

    if (_.isString(obj.auth)) {
        config.user = obj.auth.split(':')[0] || config.user;
        config.password = obj.auth.split(':')[1] || config.password;
    }

    return config;
}