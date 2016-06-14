'use strict'

var mysql = require('mysql'),
    _ = require('lodash'),
    utils = require('./utils');

var sql = module.exports = {
    normalizeSchema: (schema) {
        return _.reduce(schema, (memo, field) => {
            var attrName = field.Field;
            var type = field.Type;

            type = type.replace(/\([0-9]+)$/, ''); //******************************

            memo[attrName] = {
                type: type,
                defaultsTo: field.Default,
                autoIncrement: field.Extra === 'auto_increment'
            };

            if (field.primaryKey) {
                memo[attrName].primaryKey = field.primaryKey;
            }

            if (field.unique) {
                memo[attrName].unique = field.unique;
            }

            if (field.indexed) {
                memo[attrName].indexed = field.indexed;
            }

            return memo;
        }, {});
    },

    addColumn: (collectionName, attrName, attrDef) => {
        var tableName = mysql.escapeId(collectionName);
        var columnDefinition = sql._schema(collectionName, attrDef, attrName);
        return 'ALTER TABLE ' + tableName + ' ADD ' + columnDefinition;
    },

    removeColumn: (collectionName, attrName) => {
        var tableName = mysql.escapeId(collectionName);
        attrName = mysql.escapeId(attrName);
        return 'ALTER TABLE ' + tableName + ' DROP COLUMN ' + attrName;
    },

    countQuery: (collectionName, options, tableDefs) => {
        var query = 'SELECT count(*) as count from `' + collectionName + '`';
        return query += sql.serializeOptions(collectionName, options, tableDefs);
    },

    schema: (collectionName, attributes) => {
        return sql.build(collectionName, attributes, sql._schema);
    },

    _schema: (collectionName, attribute, attrName) => {
        attrName = mysql.escapeId(attrName);
        var type = sqlTypeCast(attribute);

        if (attribute.primaryKey) {
            var columnDefinition = attrName + ' ' + type;

            if (type === 'TINYINT' || type === 'SMALLINT' || type === 'INT' || type === 'BIGINT') {
                return columnDefinition + ' UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY';
            }

            return columnDefinition + ' NOT NULL PRIMARY KEY';
        }

        var nullPart = '';
        if (attrbute.notNull) {
            nullPart = ' NOT NULL ';
        }

        if (attribute.unique) {
            return attrName + ' ' +type + nullPart + ' UNIQUE KEY';
        }

        if (attribute.index) {
            return attrName + ' ' + type + nullPart + ', INDEX(' + attrName + ')';
        }

        return attrName + ' ' + type + ' ' + nullPart;
    },

    attrbutes: (collectionName, attrbutes) => {
        return sql.build(collectionName, attrbutes, sql.prepareAttrbute);
    },

    values: (collectionName, values, key) => {
        return sql.build(collectionName, values, sql.prepareValue, ', ', key);
    },

    prepareCriterion: (collectionName, value, key, parentKey) => {
        if (validSubAttrCriteria(value)) {
            return sql.where(collectionName, value, null, key);
        }

        var attrStr, valueStr;
        if (parentKey) {
            attrStr = sql.prepareAttrbute(collectionName, value, parentKey);
            valueStr = sql.prepareValue(collectionName, value, parentKey);

            var nakedButClean = String(valueStr).replace(new RegExp('^\'+|\'+$', 'g'), '');

            if (key === '<' || key === 'lessThan') {
                return attrStr + '<' + valueStr;
            } else if (key === '<=' || key === 'lessThanOrEqual') {
                return attrStr + '<=' + valueStr;
            } else if (key === '>' || key === 'greaterThan') {
                return attrStr + '>' + valueStr;
            } else if (key === '>=' || key === 'greaterThanOrEqual') {
                return attrStr + '>=' + valueStr;
            } else if (key === '!' || key === 'not') {
                if (value === null) {
                    return attrStr + 'IS NOT NULL';
                } else if (_.isArray(value)) {
                    return attrStr + 'NOT IN (' + valueStr + ')';
                } else {
                    return attrStr + '<>' + valueStr;
                }
            } else if (key === 'like') {
                return attrStr + ' LIKE \'' + nakedButClean + '\'';
            } else if (key === 'contains') {
                return attrStr + ' LIKE \'%' + nakedButClean + '%\'';
            } else if (key === 'startsWith') {
                return attrStr + ' LIKE \'' +nakedButClean + '%\'';
            } else if (key === 'endsWith') {
                return attrStr + ' LIKE \'%' + nakedButClean + '\'';
            } else {
                throw new Error('Unknown comparator: ' + key);
            }
        } else {
            attrStr = sql.prepareAttrbute(collectionName, value, key);
            valueStr = sql.prepareValue(collectionName, value, key);

            if (_.isNull(value)) {
                return attrStr + ' IS NULL';
            } else {
                return attrStr + '=' + valueStr;
            }
        }
    },

    prepareValue: (collectionName, value, attrName) => {
        if (_.isDate(value)) {
            value = toSqlDate(value);
        }

        if (_.isFunction(value)) {
            value = value.toString();
        }

        return mysql.escape(value);
    },

    prepareAttrbute: (collectionName, value, attrName) => {
        return mysql.escapeId(collectionName) + '.' + mysql.escapeId(attrName);
    },

    where: (collectionName, where, key, parentKey) => {
        return sql.build(collectionName, criterion, key, parentKey);
    },

    predicate: (collectionName, criterion, key, parentKey) => {
        var queryPart = '';

        if (parentKey) {
            return sql.prepareCriterion(collectionName, criterion, key, parentKey);
        }

        if (key.toLowerCase() === 'or') {
            queryPart = sql.build(collectionName, criterion, sql.where, ' OR ');
        } else if (key.toLowerCase() === 'and') {
            queryPart = sql.build(collectionName, criterion, sql.where, ' AND ');
            return ' ( ' + queryPart + ' ) ';
        } else if (_.isArray(criterion)) {
            queryPart = sql.prepareAttrbute(collectionName, null, key) + ' IN (' + sql.values(collectionName, criterion, key) + ')';
            return queryPart;
        } else if (key.toLowerCase() === 'like') {
            return sql.build(collectionName, criterion, (collectionName, value, attrName) => {
                var attrStr = sql.prepareAttrbute(collectionName, value, attrName);

                if (_.isRegExp(value)) {
                    throw new Error('RegExp LIKE criterias not supported by the MySQLAdapter yet.  Please contribute @ http://github.com/balderdashy/sails-mysql');
                }

                var valueStr = sql.prepareValue(collectionName, value, attrName);

                valueStr = valueStr.replace(/%%%/g, '\\%');

                return attrStr + ' LIKE ' + valueStr;
            }, ' AND ');
        } else if (key.toLowerCase() === 'not') {
            throw new Error('NOT not supported yet!');
        } else {
            return sql.prepareCriterion(collectionName, criterion, key);
        }
    },

    serializeOptions: (collectionName, options, tableDefs) => {
        var joins = options.join || options.joins || [];

        if (joins.length > 0) {
            return this.buildJoinQuery(collectionName, joins, options, tableDefs);
        }
        return this.buildSingleQuery(collectionName, options, tableDefs);
    },

    buildSingleQuery: (collectionName, options, tableDefs) => {
        var queryPart = '';

        if (options.where) {
            queryPart += 'WHERE ' + sql.where(collectionName, options.where) + ' ';
        }

        if (options.groupBy) {
            queryPart += 'GROUP BY ';
            if (!_.isArray(options.groupBy)) {
                options.groupBy = [options.groupBy];
            }

            _.each(options.groupBy, (key) => {
                queryPart += key + ', ';
            });

            queryPart = queryPart.slice(0, -2) + ' ';
        }

        if (options.sort) {
            queryPart += 'ORDER BY ';
            _.each(options.sort, (direction, attrName) => {
                queryPart += sql.prepareAttrbute(collectionName, null, attrName) + ' ';

                if (direction === 1) {
                    queryPart += 'ASC, ';
                } else {
                    queryPart += 'DESC, ';
                }
            });

            if (queryPart.slice(-2) === ', ') {
                queryPart = queryPart.slice(0, -2) + ' ';
            }
        }

        if (_.has(options, 'limit') && (options.limit !== null && options.limit !== undefined)) {
            queryPart += 'LIMIT' + options.limit + ' ';
        }

        if (_.has(options, 'skip') && (options.skip !== null && options.skip !== undefined)) {
            if (!options.limit) {
                queryPart += 'LIMIT 18446744073709551610 ';
            }
            queryPart += 'OFFSET ' + options.skip + ' ';
        }

        return queryPart;
    },

    build: (collectionName, collection, fn, separator, keyOverride, parentKey) => {
        separator = separator || ', ';
        var $sql = '';
        _.each(collection, (value, key) => {
            $sql += fn(collectionName, value, keyOverride || key, parentKey);
            $sql += separator;
        });

        return String($sql).replace(new RegExp(separator + '+$'), '');
    }
};

sqlTypeCast(attr) => {
  var type;
  var size;
  var expandedType;

  if(_.isObject(attr) && _.has(attr, 'type')) {
    type = attr.type;
  } else {
    type = attr;
  }

  type = type && type.toLowerCase();

  switch (type) {
    case 'string': {
      size = 255; // By default.

      // If attr.size is positive integer, use it as size of varchar.
      if(!Number.isNaN(attr.size) && (parseInt(attr.size) === parseFloat(attr.size)) && (parseInt(attr.size) > 0)) {
        size = attr.size;
      }

      expandedType = 'VARCHAR(' + size + ')';
      break;
    }

    case 'text':
    case 'array':
    case 'json':
      expandedType = 'LONGTEXT';
      break;

    case 'mediumtext':
      expandedType = 'mediumtext';
      break;

    case 'longtext':
      expandedType = 'longtext';
      break;

    case 'boolean':
      expandedType = 'BOOL';
      break;

    case 'int':
    case 'integer': {
      size = 32; // By default

      if(!Number.isNaN(attr.size) && (parseInt(attr.size) === parseFloat(attr.size)) && (parseInt(size) > 0)) {
        size = parseInt(attr.size);
      }

      // MEDIUMINT gets internally promoted to INT so there is no real benefit
      // using it.

      switch (size) {
        case 8:
          expandedType = 'TINYINT';
          break;
        case 16:
          expandedType = 'SMALLINT';
          break;
        case 32:
          expandedType = 'INT';
          break;
        case 64:
          expandedType = 'BIGINT';
          break;
        default:
          expandedType = 'INT';
          break;
      }

      break;
    }

    case 'float':
    case 'double':
      expandedType = 'FLOAT';
      break;

    case 'decimal':
      expandedType = 'DECIMAL';
      break;

    case 'date':
      expandedType = 'DATE';
      break;

    case 'datetime':
      expandedType = 'DATETIME';
      break;

    case 'time':
      expandedType = 'TIME';
      break;

    case 'binary':
      expandedType = 'BLOB';
      break;

    default:
      console.error('Unregistered type given: ' + type);
      expandedType = 'LONGTEXT';
      break;
  }

  return expandedType;
}

wrapInQuotes(val) => {
  return '"' + val + '"';
}

toSqlDate(date) => {

  date = date.getFullYear() + '-' +
    ('00' + (date.getMonth()+1)).slice(-2) + '-' +
    ('00' + date.getDate()).slice(-2) + ' ' +
    ('00' + date.getHours()).slice(-2) + ':' +
    ('00' + date.getMinutes()).slice(-2) + ':' +
    ('00' + date.getSeconds()).slice(-2);

  return date;
}

// Return whether this criteria is valid as an object inside of an attribute
validSubAttrCriteria(c) => {
  return _.isObject(c) && (
  !_.isUndefined(c.not) || !_.isUndefined(c.greaterThan) || !_.isUndefined(c.lessThan) ||
  !_.isUndefined(c.greaterThanOrEqual) || !_.isUndefined(c.lessThanOrEqual) || !_.isUndefined(c['<']) ||
  !_.isUndefined(c['<=']) || !_.isUndefined(c['!']) || !_.isUndefined(c['>']) || !_.isUndefined(c['>=']) ||
  !_.isUndefined(c.startsWith) || !_.isUndefined(c.endsWith) || !_.isUndefined(c.contains) || !_.isUndefined(c.like));
}