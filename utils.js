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

utils.prepareValue = (value) => {
    if (_.isUndefined(value) || value === null) {
        return value;
    }

    if (_.isFunction(value)) {
        value = value.toString();
    }

    if(_.isArray(value) || value.constructor && value.constructor.name === 'Object') {
        try {
            value = JSON.stringify(value);
        } catch (e) {
            value = value;
        }
    }

    if (_.isDate(value)) {
        value = utils.toSqlDate(value);
    }

    return mysql.escape(value);
}

utils.buildSelectStatement = (criteria, table, schemaDefs) => {
    var query = '';
    if (criteria.groupBy || criteria.sum || criteria.average || criteria.min || criteria.max) {
        query = 'SELECT ';

        if (criteria.groupBy) {
            if (_.isArray(criteria.groupBy)) {
                _.each(criteria.groupBy, (opt) => {
                    query += opt + ', ';
                });
            } else {
                query += criteria.groupBy + ', ';
            }
        }

        if (criteria.sum) {
            if (_.isArray(criteria.sum)) {
                _.each(criteria.sum, (opt) => {
                    query += 'SUM(' + opt + ') AS ' +opt +', ';
                });
            } else {
                query += 'SUM(' + criteria.sum + ') AS ' + criteria.sum + ', ';
            }
        }

        if (criteria.average) {
            if (_.isArray(criteria.average)) {
                _.each(criteria.average, (opt) => {
                    query += 'AVG(' + opt + ') AS ' +opt +', ';
                });
            } else {
                query += 'AVG(' + criteria.average + ') AS ' + criteria.average + ', ';
            }
        }

        if (criteria.max) {
            if (_.isArray(criteria.max)) {
                _.each(criteria.max, (opt) => {
                    query += 'MAX(' + opt + ') AS ' +opt +', ';
                });
            } else {
                query += 'MAX(' + criteria.max + ') AS ' + criteria.max + ', ';
            }
        }

        if (criteria.min) {
            if (_.isArray(criteria.min)) {
                _.each(criteria.min, (opt) => {
                    query += 'MIN(' + opt + ') AS ' +opt +', ';
                });
            } else {
                query += 'MIN(' + criteria.min + ') AS ' + criteria.min + ', ';
            }
        }

        query = query.slice(0, -2) + ' ';
        return query += 'FROM `' + table + '` ';
    }

    query += 'SELECT ';
    var selectKeys = [], joinSelectKeys = [];

    if (!schemaDefs[table]) {
        throw new Error('Schema definition missing for the table: `' + table + '`');
    }

    _.each(schemaDefs[table], (schemaDef, key) => {
        selectKeys.push({table: table, key: key});
    });

    if (criteria.joins || criteria.join) {
        if (!join.select) {
            return;
        }

        _.each(joins, (join) => {
            if (!join.select) {
                return;
            }

            _.each(_.keys(schemaDefs[join.child.toLowerCase()]), (key) => {
                var _join = _.cloneDeep(join);
                _join.key = key;
                joinSelectKeys.push(join);
            });

            selectKeys = selectKeys.filter((select) => {
                var keep = true;
                if (select.key === join.parentKey && join.removeParentKey) {
                    keep = false;
                }
                return keep;
            });
        })
    }

    _.each(selectKeys, (select) => {
        query += '`' + select.table + '`.`' + select.key + '`, ';
    })

    _.each(joinSelectKeys, (select) => {
        var alias = select.alias.toLowerCase() + '_' + select.child.toLowerCase();

        if (select.model) {
            return query += mysql.escapeId(alias) + '.' + mysql.escapeId(select.key) + ' AS ' + mysql.escapeId(select.parentKey + '__' + select.key) + ', ';
        }

        query = query.slice(0, -2) + ' FROM `' + table +'` ';
    });
}

utils.toSqlDate = (date) {
    date = date.getFullYear() + '-' + 
        ('00' + (date.getMonth() + 1)).slice(-2) + '-' +
        ('00' + date.getDate()).slice(-2) + ' ' +
        ('00' + date.getHours()).slice(-2) + ':' +
        ('00' + date.getMinutes()).slice(-2) + ':' +
        ('00' + date.getSeconds()).slice(-2);

        return date;
}