"use strict";
const axios = require('axios');
const https = require('https')
const async = require('async');
const moment = require('moment')
const pluralize = require('pluralize')
const logger = require('./winston')
const URI = require('urijs');
const _ = require('lodash');
const { v5: uuid5 } = require('uuid');
const FHIRPath = require('fhirpath');
const sqlstring = require('sqlstring');
const { Sequelize, DataTypes, QueryTypes } = require('sequelize');


/**
 * Disable only in development mode
 */
const httpsAgent = new https.Agent({
  rejectUnauthorized: false,
})
axios.defaults.httpsAgent = httpsAgent

class CacheFhirToES {
  constructor({
    FHIRBaseURL,
    FHIRUsername,
    FHIRPassword,
    relationshipsIDs = [],
    since,
    reset = false,
    ESModulesBasePath,
    DBConnection
  }) {
    this.FHIRBaseURL = FHIRBaseURL
    this.FHIRUsername = FHIRUsername
    this.FHIRPassword = FHIRPassword
    this.relationshipsIDs = relationshipsIDs
    this.since = since
    this.reset = reset
    this.deletedRelatedDocs = []
    this.ESModulesBasePath = ESModulesBasePath
    this.sequelize = new Sequelize(DBConnection.database, DBConnection.username, DBConnection.password, {
      host: 'localhost',
      dialect: DBConnection.dialect
    });
  }
  getRelationshipFields(relationship) {
    let fields = []
    for(let ext of relationship.extension) {
      let elements = ext.extension.filter((ext) => {
        return ext.url === "http://ihris.org/fhir/StructureDefinition/iHRISReportElement"
      })
      for(let element of elements) {
        let name = element.extension.find((ext) => {
          return ext.url === "name"
        })?.valueString
        let type = element.extension.find((ext) => {
          return ext.url === "type"
        })?.valueString
        if(name) {
          fields.push({
            name,
            type
          })
        }
      }
    }
    return fields
  }
  flattenComplex(extension) {
    let results = {};
    for (let ext of extension) {
      let value = '';
      for (let key of Object.keys(ext)) {
        if (key !== 'url') {
          value = ext[key];
        }
      }
      if (results[ext.url]) {
        if (Array.isArray(results[ext.url])) {
          results[ext.url].push(value);
        } else {
          results[ext.url] = [results[ext.url], value];
        }
      } else {
        if (Array.isArray(value)) {
          results[ext.url] = [value];
        } else {
          results[ext.url] = value;
        }
      }
    }
    return results;
  };

  /**
   *
   * @param {relativeURL} reference //reference must be a relative url i.e Practioner/10
   */
  getResourceFromReference(reference) {
    return new Promise((resolve) => {
      let url = URI(this.FHIRBaseURL)
        .segment(reference)
        .toString()
      axios.get(url, {
        headers: {
          'Cache-Control': 'no-cache',
        },
        withCredentials: true,
        auth: {
          username: this.FHIRUsername,
          password: this.FHIRPassword
        },
      }).then(response => {
        return resolve(response.data)
      }).catch((err) => {
        logger.error('Error occured while getting resource reference');
        logger.error(err);
        return resolve()
      })
    }).catch((err) => {
      logger.error('Error occured while getting resource reference');
      logger.error(err);
    })
  }

  resourceDisplayName(resource, displayFormat) {
    return new Promise((resolve) => {
      let details = {}
      let valformat = displayFormat && displayFormat.extension.find((ext) => {
        return ext.url === 'format'
      })
      if(valformat) {
        details.format = valformat.valueString
      }
      let valorder = displayFormat && displayFormat.extension.find((ext) => {
        return ext.url === 'order'
      })
      let paths = displayFormat && displayFormat.extension.filter((ext) => {
        return ext.url.startsWith("paths:")
      })
      if(valorder) {
        details.order = valorder.valueString
      } else {
        if(paths) {
          for(let path of paths) {
            path = path.url.split(":")
            if(path.length > 1) {
              if(!details.order) {
                details.order = path[1]
              } else {
                details.order += ',' + path[1]
              }
            }
          }
        }
      }
      if(paths && paths.length > 0) {
        details.paths = {}
        for(let path of paths) {
          let url = path.url.split(":")
          if(url.length > 1) {
            if(!details.paths[url[1]]) {
              details.paths[url[1]] = {}
            }
            details.paths[url[1]][url[2]] = path.valueString
          }
        }
      }
      if(!details.order && !details.paths) {
        let name = resource.name
        if(!name) {
          name = resource.extension && resource.extension.find((ext) => {
            return ext.url === 'http://ihris.org/fhir/StructureDefinition/ihris-basic-name'
          })
          if(name) {
            name = name.valueString
          }
        }
        return resolve(name)
      }
      let format = details.format || "%s"
      let output = []
      let order = details.order.split(',')
      if ( details.fhirpath ) {
        output.push( FHIRPath.evaluate( resource, details.fhirpath ).join( details.join || " " ) )
      } else if ( details.paths ) {
        for ( let ord of order ) {
          ord = ord.trim()
          output.push( FHIRPath.evaluate( resource, details.paths[ ord ].fhirpath ).join( details.paths[ord].join || " " ) )
        }
      }
      for(let val of output) {
        format = format.replace('%s', val)
      }
      return resolve(format)
    })
  }

  /**
   *
   * @param {Array} extension
   * @param {String} element
   */
  getElementValFromExtension(extension, element) {
    return new Promise((resolve) => {
      let elementValue = ''
      async.each(extension, (ext, nxtExt) => {
        let value
        for (let key of Object.keys(ext)) {
          if (key !== 'url') {
            value = ext[key];
          }
        }
        if (ext.url === element) {
          elementValue = value
        }
        (async () => {
          if (Array.isArray(value)) {
            let val
            try {
              val = await this.getElementValFromExtension(value, element)
            } catch (error) {
              logger.error(error);
            }
            if (val) {
              elementValue = val
            }
            return nxtExt()
          } else {
            return nxtExt()
          }
        })();
      }, () => {
        resolve(elementValue)
      })
    }).catch((err) => {
      logger.error('Error occured while geting Element value from extension');
      logger.error(err);
    })
  }

  getImmediateLinks(links, callback) {
    if (this.orderedResources.length - 1 === links.length) {
      return callback(this.orderedResources);
    }
    let promises = [];
    for (let link of links) {
      promises.push(
        new Promise((resolve, reject) => {
          link = this.flattenComplex(link.extension);
          let parentOrdered = this.orderedResources.find(orderedResource => {
            let linkToResource = link.linkTo.split('.').shift()
            return orderedResource.name === linkToResource;
          });
          let exists = this.orderedResources.find(orderedResource => {
            return JSON.stringify(orderedResource) === JSON.stringify(link);
          });
          if (parentOrdered && !exists) {
            this.orderedResources.push(link);
          }
          resolve();
        })
      );
    }
    Promise.all(promises).then(() => {
      if (this.orderedResources.length - 1 !== links.length) {
        this.getImmediateLinks(links, () => {
          return callback();
        });
      } else {
        return callback();
      }
    }).catch((err) => {
      logger.error(err);
      return callback();
    });
  };

  getReportRelationship(callback) {
    let url = URI(this.FHIRBaseURL)
      .segment('Basic')
    if(this.relationshipsIDs.length > 0) {
      url.addQuery('_id', this.relationshipsIDs.join(','));
    } else {
      url.addQuery('code', 'iHRISRelationship');
    }
    url = url.toString();
    axios
      .get(url, {
        headers: {
          'Cache-Control': 'no-cache',
        },
        withCredentials: true,
        auth: {
          username: this.FHIRUsername,
          password: this.FHIRPassword,
        },
      })
      .then(relationships => {
        return callback(false, relationships.data);
      })
      .catch(err => {
        logger.error(err);
        return callback(err, false);
      });
  };

  createSyncDataTable() {
    return new Promise((resolve, reject) => {
      let syncdatafields = {
        id: {
          type: DataTypes.STRING,
          allowNull: false,
          primaryKey: true
        },
        last_began_indexing_time: {
          type: DataTypes.DATE
        },
        last_ended_indexing_time: {
          type: DataTypes.DATE
        }
      }
      let table = this.sequelize.define("fhir2sqlsyncdata", syncdatafields, {
        timestamps: false
      })
      table.sync().then(() => {
        resolve()
      }).catch((err) => {
        logger.error(err);
        reject()
      })
    })
  }

  updateLastIndexingTime(start, ended, index) {
    return new Promise((resolve, reject) => {
      logger.info('Updating lastIndexingTime')
      let sql = `update fhir2sqlsyncdata set last_began_indexing_time='${start}', last_ended_indexing_time='${ended}' where id='${index}'`
      this.executeSQL(sql, "UPDATE").then((response) => {
        if(response[1] == 0) {
          let sql = `insert into fhir2sqlsyncdata values ('${index}', '${start}', '${ended}')`
          this.executeSQL(sql, "INSERT").then(() => {
            return resolve(false)
          }).catch((err) => {
            logger.error(err)
            logger.error('An error occured while updating lastIndexingTime')
            return reject(true)
          })
        } else {
          return resolve(false)
        }
      }).catch((err) => {
        logger.error(err)
        logger.error('An error occured while updating lastIndexingTime')
        return reject(true)
      })
    })
  }

  getLastIndexingTime(index) {
    return new Promise((resolve, reject) => {
      if(this.since && !this.reset) {
        this.last_began_indexing_time = this.since
        this.last_ended_indexing_time = this.since
        return resolve()
      }
      logger.info('Getting lastIndexingTime')
      let sql = `select * from fhir2sqlsyncdata where id='${index}'`
      this.executeSQL(sql, 'SELECT').then((response) => {
        if(this.reset) {
          logger.info('Returning lastIndexingTime of 1970-01-01T00:00:00')
          this.last_began_indexing_time = '1970-01-01T00:00:00'
          this.last_ended_indexing_time = '1970-01-01T00:00:00'
          return resolve()
        }
        if(response.length === 0) {
          logger.info('Returning lastIndexingTime of 1970-01-01T00:00:00')
          this.last_began_indexing_time = '1970-01-01T00:00:00'
          this.last_ended_indexing_time = '1970-01-01T00:00:00'
          return resolve()
        }
        logger.info('Returning last_began_indexing_time of ' + response[0].last_began_indexing_time)
        this.last_began_indexing_time = moment(response[0].last_began_indexing_time).format("YYYY-MM-DDTHH:mm:ss")
        this.last_ended_indexing_time = moment(response[0].last_ended_indexing_time).format("YYYY-MM-DDTHH:mm:ss")
        return resolve()
      }).catch((err) => {
        logger.error(err);
        logger.error('Error occured while getting last indexing time in ES');
        logger.info('Returning last_began_indexing_time of 1970-01-01T00:00:00')
        this.last_began_indexing_time = '1970-01-01T00:00:00'
        this.last_ended_indexing_time = '1970-01-01T00:00:00'
        return reject()
      })
    })
  }

  formatColumn(name) {
    if(name.startsWith('__') || name == 'id') {
      return name
    }
    name = name.toLowerCase()
    name = 'ihris_' + name
    name = name.split("-").join("_")
    return name
  }

  createTable(name, IDFields, relationship) {
    return new Promise((resolve, reject) => {
      let relfields = [...IDFields, ...this.getRelationshipFields(relationship)]
      let fields = {
        id: {
          type: DataTypes.STRING,
          allowNull: false,
          primaryKey: true
        }
      }
      for(let field of relfields) {
        let fieldname = this.formatColumn(field.name)
        let type = 'TEXT'
        if(field.type === 'string') {
          type = 'TEXT'
        } else if(field.type === 'date') {
          type = 'DATEONLY'
        } else if(field.type === 'datetime' || field.type === 'dateTime' || field.type === 'DateTime') {
          type = 'DATE'
        }
        fields[fieldname] = {
          type: DataTypes[type]
        }
      }
      fields.lastupdated = {
        type: DataTypes.DATE
      }
      let table = this.sequelize.define(name, fields, {
        timestamps: false
      })
      table.sync({ alter: true }).then(() => {
        resolve()
      }).catch((err) => {
        logger.error(err);
        reject()
      })
    })
  }

  getQueryCondition(data) {
    let where = ''
    if(data.query.terms) {
      let condition = Object.keys(data.query.terms)[0]
      let conditionVal = data.query.terms[condition]
      condition = condition.replace(".keyword", "")
      if(condition) {
        condition = this.formatColumn(condition)
      }
      if(Array.isArray(conditionVal)) {
        conditionVal = conditionVal[0]
      }
      if(condition && conditionVal) {
        where += ` where ${condition}='${conditionVal}'`
      }
    } else if(data.query.bool) {
      if(Array.isArray(data.query.bool.should)) {
        for(let should of data.query.bool.should) {
          let where_should = ''
          let terms
          if(should.terms) {
            terms = should.terms
          } else if(should.term) {
            terms = should.term
          } else if(should.match) {
            terms = should.match
          }
          let key = Object.keys(terms)[0]
          let column = key.replace(".keyword", "")
          if(column) {
            column = this.formatColumn(column)
          }
          if(Array.isArray(terms[key])) {
            for(let term of terms[key]) {
              if(!where_should) {
                where_should = ` (`
              } else {
                where_should += ` or `
              }
              where_should += `${column}='${term}'`
            }
            if(where_should) {
              where_should += ')'
            }
          } else {
            where_should += ` (${column}='${terms[key]}')`
          }
          if(where) {
            where += ` or ${where_should}`
          } else {
            where = ` where ${where_should}`
          }
        }
      }
      if(Array.isArray(data.query.bool.must)) {
        for(let must of data.query.bool.must) {
          let where_must = ''
          let terms
          if(must.terms) {
            terms = must.terms
          } else if(must.term) {
            terms = must.term
          } else if(must.match) {
            terms = must.match
          }
          let key = Object.keys(terms)[0]
          let column = key.replace(".keyword", "")
          if(column) {
            column = this.formatColumn(column)
          }
          if(Array.isArray(terms[key])) {
            for(let term of terms[key]) {
              if(!where_must) {
                where_must = ` (`
              } else {
                where_must += ` or `
              }
              where_must += `${column}='${term}'`
            }
            if(where_must) {
              where_must += ')'
            }
          } else {
            where_must += ` (${column}='${terms[key]}')`
          }
          if(where) {
            where += ` and ${where_must}`
          } else {
            where = ` where ${where_must}`
          }
        }
      }
      if(Array.isArray(data.query.bool.must_not)) {
        for(let must_not of data.query.bool.must_not) {
          let where_must_not = ''
          let terms
          if(must_not.terms) {
            terms = must_not.terms
          } else if(must_not.term) {
            terms = must_not.term
          } else if(must_not.match) {
            terms = must_not.match
          } else if(must_not.exists) {
            let column = this.formatColumn(must_not.exists.field)
            where_must_not = ` (${column} is NULL or ${column}='')`
            if(where) {
              where += ` and ${where_must_not}`
            } else {
              where += ` where ${where_must_not}`
            }
            break
          }
          let key = Object.keys(terms)[0]
          let column = key.replace(".keyword", "")
          if(column) {
            column = this.formatColumn(column)
          }
          if(Array.isArray(terms[key])) {
            for(let term of terms[key]) {
              if(!where_must_not) {
                where_must_not = ` (`
              } else {
                where_must_not += ` or `
              }
              where_must_not += `${column}!='${term}'`
            }
            if(where_must_not) {
              where_must_not += ')'
            }
          } else {
            where_must_not += ` (${column}!='${terms[key]}')`
          }
          if(where) {
            where += ` and ${where_must_not}`
          } else {
            where += ` where ${where_must_not}`
          }
        }
      }
    }
    return where
  }

  generateSelectSQL(data, index) {
    index = pluralize(index)
    let where = this.getQueryCondition(data)
    let query = `select * from ${index} ${where}`
    return query
  }

  generateDeleteSQL(data, index) {
    index = pluralize(index)
    let where = this.getQueryCondition(data)
    let query = `delete from ${index} ${where}`
    return query
  }

  generateUpdateSQL(data, index) {
    index = pluralize(index)
    let query = `update ${index} set `
    if(data.script && data.script.source) {
      let sources = data.script.source.split(";")
      for(let source of sources) {
        let src = source.split("=")
        let value = src[1]
        let col = src[0].split("'")[1]
        if(!col) {
          continue
        }
        col = this.formatColumn(col)
        query += `${col}=${value.trim()}, `
      }
      query += `lastUpdated='${moment().format("Y-MM-DDTHH:mm:ss")}'`
      let where = this.getQueryCondition(data)
      query += where
    } else if(typeof data === 'object') {
      for(let dt in data) {
        let column = this.formatColumn(dt)
        query += `${column}=${data[dt].trim()} `
      }
    }
    return query
  }

  generateInsertSQL(data, index) {
    index = pluralize(index)
    let query = `insert into ${index} `
    let columns = ''
    let values = ''
    if(typeof data === 'object') {
      delete data.lastUpdated
      for(let dt in data) {
        let column = this.formatColumn(dt)
        if(!columns) {
          columns = `(${column}`
        } else {
          columns += `, ${column}`
        }
        let value = data[dt]
        if(data[dt]) {
          value = sqlstring.escape(data[dt].trim())
        }
        if(!values) {
          values = `(${value}`
        } else {
          values += `, ${value}`
        }
      }
    } else {
      let sources = data.split(";")
      for(let source of sources) {
        let src = source.split("=")
        let value = sqlstring.escape(src[1].trim())
        let col = src[0].split("'")[1]
        let column = this.formatColumn(col)
        if(!columns) {
          columns = `(${column}`
        } else {
          columns += `, ${column}`
        }
        if(!values) {
          values = `(${value}`
        } else {
          values += `, ${value}`
        }
      }
    }
    let time = moment().format("Y-MM-DDTHH:mm:ss")
    columns += `, lastUpdated)`
    values += `, '${time}')`
    query += `${columns} values ${values}`
    return query
  }

  executeSQL(sql, type) {
    return new Promise((resolve, reject) => {
      if(type) {
        type = { type: QueryTypes[type] }
      }
      this.sequelize.query(sql, type).then((response) => {
        resolve(response)
      }).catch((err) => {
        logger.error(err);
        reject()
      })
    })
  }

  getResourcesFields(resources) {
    let fields = []
    for(let resource of resources) {
      if(resource["http://ihris.org/fhir/StructureDefinition/iHRISReportElement"]) {
        for (let element of resource["http://ihris.org/fhir/StructureDefinition/iHRISReportElement"]) {
          let fieldName
          let fhirPathValue
          let fieldAutogenerated = false
          for (let el of element) {
            let value = '';
            for (let key of Object.keys(el)) {
              if (key !== 'url') {
                value = el[key];
              }
            }
            if (el.url === "name") {
              let fleldChars = value.split(' ')
              //if name has space then format it
              if (fleldChars.length > 1) {
                fieldName = value.toLowerCase().split(' ').map(word => word.replace(word[0], word[0].toUpperCase())).join('');
              } else {
                fieldName = value
              }
            } else if (el.url === "fhirpath") {
              fhirPathValue = value
            } else if (el.url === "autoGenerated") {
              fieldAutogenerated = value
            }
          }
          fields.push({
            resourceName: resource.name,
            resourceType: resource.resource,
            field: fieldName,
            fhirPath: fhirPathValue,
            fieldAutogenerated
          })
        }
        fields.push({
          resourceName: resource.name,
          resourceType: resource.resource,
          field: resource.name,
          fhirPath: "id",
          fieldAutogenerated: false
        })
      }
    }
    return fields;
  }

  getChildrenResources(resourceName) {
    let childrenResources = []
    for(let orderedResource of this.orderedResources) {
      if(orderedResource.linkTo === resourceName || (orderedResource.linkTo && orderedResource.linkTo.startsWith(resourceName + '.'))) {
        childrenResources.push(orderedResource)
        let grandChildren = this.getChildrenResources(orderedResource.name)
        childrenResources = childrenResources.concat(grandChildren)
      }
    }
    return childrenResources
  }

  async updateESDocument(body, record, index, orderedResource, resourceData, tryDeleting, extraTerms, callback) {
    let multiple = orderedResource.multiple
    let allTerms = _.cloneDeep(body.query.terms)
    //this handles records that should be deleted instead of its fields being truncated
    let recordDeleted = false;
    async.series({
      addNewRows: (callback) => {
        //ensure that this is not the primary resource and has multiple tag, otherwise return
        if(!orderedResource.hasOwnProperty('linkElement') || !multiple) {
          return callback(null)
        }
        let query = {
          query: {
            bool: {
              must: [{
                terms: body.query.terms
              }]
            }
          }
        }
        query.query.bool.must = query.query.bool.must.concat(extraTerms)
        let sqlquery = this.generateSelectSQL(query, index)
        this.executeSQL(sqlquery, 'SELECT').then(async(results) => {
          //if field values for this record needs to be truncated and there are multiple records of the parent, then delete the one we are truncating instead of updating
          if(results.length > 1 && tryDeleting) {
            let recordFields = Object.keys(record)
            let idField = recordFields[recordFields.length - 1]
            let termField = Object.keys(body.query.terms)[0]
            let delQry = {
              query: {
                bool: {
                  must: []
                }
              }
            }
            let must1 = {
              terms: {}
            }
            must1.terms[termField] = body.query.terms[termField]
            delQry.query.bool.must.push(must1)
            let must2 = {
              terms: {}
            }
            must2.terms[idField] = [record[idField]]
            delQry.query.bool.must.push(must2)
            delQry.query.bool.must = delQry.query.bool.must.concat(extraTerms)
            try {
              let sqlquery = this.generateDeleteSQL(delQry, index)
              await this.executeSQL(sqlquery)
            } catch (error) {
              logger.error(error);
            }
            recordDeleted = true;
          }
          if(recordDeleted || tryDeleting) {
            return callback(null)
          }
          //because this resource supports multiple rows, then we are trying to add new rows
          let newRows = []
          // take the last field because it is the ID
          let recordFields = Object.keys(record)
          let checkField = recordFields[recordFields.length - 1]

          for(let linkField in allTerms) {
            for(let index in allTerms[linkField]) {
              let newRowBody = {}
              // create new row only if there is no checkField or checkField exist but it is different
              let updateThis = results.find((hit) => {
                return hit[linkField] === allTerms[linkField][index] && (!hit[checkField] || hit[checkField] === record[checkField])
              })
              if(!updateThis) {
                let hit = results.find((hit) => {
                  return hit[linkField] === allTerms[linkField][index]
                })
                if(!hit) {
                  continue;
                }
                for(let field in hit) {
                  newRowBody[field] = hit[field]
                }
                for(let recField in record) {
                  newRowBody[recField] = record[recField]
                }
                let trmInd = body.query.terms[linkField].findIndex((trm) => {
                  return trm === allTerms[linkField][index]
                })
                body.query.terms[linkField].splice(trmInd, 1)
              }
              if(Object.keys(newRowBody).length > 0) {
                newRows.push(newRowBody)
              }
            }
          }
          if(newRows.length > 0) {
            async.eachSeries(newRows, (newRowBody, nxt) => {
              newRowBody.lastUpdated = moment().format('Y-MM-DDTHH:mm:ss');
              let query = this.generateInsertSQL(newRowBody, index)
              this.executeSQL(query).then(() => {
                return nxt()
              }).catch(() => {
                return nxt()
              })
            }, () => {
              return callback(null)
            })
          } else {
            return callback(null)
          }
        })
      },
      updateRow: (callback) => {
        if(recordDeleted) {
          return callback(null);
        }
        // for multiple rows, ensure that we dont update all rows but just one row
        let bodyData = {}
        let recordFields = Object.keys(record)
        let idField = recordFields[recordFields.length - 1]
        if(multiple) {
          let termField = Object.keys(body.query.terms)[0]
          bodyData = {
            query: {
              bool: {
                must: []
              }
            }
          }
          let must1 = {
            terms: {}
          }
          must1.terms[termField] = body.query.terms[termField]
          bodyData.query.bool.must.push(must1)
          let must2 = {
            terms: {}
          }
          must2.terms[idField] = [record[idField]]
          bodyData.query.bool.must.push(must2)
          bodyData.query.bool.must = bodyData.query.bool.must.concat(extraTerms)
          bodyData.script = body.script
        } else {
          bodyData = body
        }
        async.series({
          updateDocMissingField: (callback) => {
            if(!multiple) {
              return callback(null)
            }
            let updBodyData = _.cloneDeep(bodyData)
            updBodyData.script.source += `ctx._source.lastUpdated='${moment().format("Y-MM-DDTHH:mm:ss")}';`
            updBodyData.query.bool.must.splice(1, 1)

            let must_not = {
              exists: {}
            }
            must_not.exists.field = idField
            updBodyData.query.bool.must_not = [must_not]
            let query = this.generateUpdateSQL(updBodyData, index)
            this.executeSQL(query).then(() => {
              return callback(null)
            }).catch(() => {
              return callback(null)
            })
          },
          updateDocHavingField: (callback) => {
            let updBodyData = _.cloneDeep(bodyData)
            updBodyData.script.source += `ctx._source.lastUpdated='${moment().format("Y-MM-DDTHH:mm:ss")}';`
            let query = this.generateUpdateSQL(updBodyData, index)
            this.executeSQL(query).then((response) => {
              // if nothing was updated and its from the primary (top) resource then create as new
              if (response[1].rowCount == 0 && !orderedResource.hasOwnProperty('linkElement')) {
                logger.info('No record with id ' + resourceData.id + ' found on elastic search, creating new');
                let id = record[orderedResource.name].split('/')[1]
                if(extraTerms.length > 0) {
                  let ids = [id]
                  for(let extraTerm of extraTerms) {
                    ids.push(Object.values(extraTerm.terms)[0][0].split('/')[1])
                  }
                  id = this.generateId(ids)
                }
                let recordData = _.cloneDeep(record)
                recordData.lastUpdated = moment().format("Y-MM-DDTHH:mm:ss");
                recordData.id = id
                let query = this.generateInsertSQL(recordData, index)
                this.executeSQL(query).then(() => {
                  return callback(null)
                }).catch(() => {
                  return callback(null)
                })
              } else {
                return callback(null)
              }
            }).catch(() => {
              return callback(null)
            })
          }
        }, () => {
          return callback(null)
        })
      },
      cleanBrokenLinks: (callback) => {
        if(!orderedResource.hasOwnProperty('linkElement') || (resourceData.meta && resourceData.meta.versionId == '1')) {
          return callback(null)
        }
        // get all documents that doesnt meet the search terms but are linked to this resource and truncate
        let qry = {
          query: {
            bool: {
              must_not: [],
              must: []
            }
          }
        }
        qry.query.bool.must_not = [{
          terms: allTerms
        }]
        let recordFields = Object.keys(record)
        let idField = recordFields[recordFields.length - 1]
        let term = {}
        term[idField] = record[idField]
        qry.query.bool.must = [{
          term
        }]
        //include extra terms
        qry.query.bool.must = qry.query.bool.must.concat(extraTerms)
        let childrenResources = this.getChildrenResources(orderedResource.name);
        childrenResources.unshift(orderedResource)
        let fields = this.getResourcesFields(childrenResources)
        let ctx = ''
        for(let field of fields) {
          ctx += `ctx._source['${field.field}']=null;`;
        }
        ctx += `ctx._source.lastUpdated='${moment().format('Y-MM-DDTHH:mm:ss')}';`
        let sqlquery = this.generateSelectSQL(qry, index)
        this.executeSQL(sqlquery, 'SELECT').then(async(results) => {
          let queryRelated = {
            query: {
              bool: {
                should: []
              }
            }
          }
          //save fields that are used to test if two docs are the same
          let compFieldsRelDocs = []
          let ids = []
          for(let hit of results) {
            ids.push(hit._id)
            //if relationship supports multiple rows then construct a query that can get all other documents that has the same field as this,
            //if we find one then we delete instead of truncate
            if(orderedResource.multiple) {
              const name = orderedResource.name
              let link = '__' + name + '_link'
              let query = {
                bool: {
                  must: [],
                  must_not: []
                }
              }
              for(let field in hit) {
                let resourceField = orderedResource['http://ihris.org/fhir/StructureDefinition/iHRISReportElement'].find((elements) => {
                  return elements.find((element) => {
                    return element.url === 'name' && element.valueString === field
                  })
                })
                if(field === name || field === link || resourceField || field === 'lastUpdated') {
                  continue
                }
                let exist = compFieldsRelDocs.find((fld) => {
                  return fld === field
                })
                if(!exist) {
                  compFieldsRelDocs.push(field)
                }
                if(hit[field] === null) {
                  let must_not = {
                    exists: {
                      field
                    }
                  }
                  query.bool.must_not.push(must_not)
                } else {
                  let must = {
                    match: {}
                  }
                  must.match[field] = hit[field]
                  query.bool.must.push(must)
                }
              }
              queryRelated.query.bool.should.push(query)
            }
          }
          if(ids.length > 0) {
            let relatedDocs = []
            let getRelatedDocs = new Promise((resolve) => {
              if(!orderedResource.multiple) {
                return resolve()
              }
              let sqlquery = this.generateSelectSQL(queryRelated, index)
              this.executeSQL(sqlquery, 'SELECT').then((results) => {
                relatedDocs = results
                return resolve()
              })
            })
            getRelatedDocs.then(() => {
              //separate IDs whose docs should be truncated with those that should be deleted
              let deleteIDs = []
              let truncateIDs = []
              for(let doc1 of relatedDocs) {
                for(let doc2 of relatedDocs) {
                  if(doc1._id === doc2._id) {
                    continue
                  }
                  let same = true
                  for(let field of compFieldsRelDocs) {
                    if(doc1[field] !== doc2[field]) {
                      same = false
                      break
                    }
                  }
                  if(same) {
                    let totalDeletedRelDocs = 0
                    for(let deletedRelDoc of this.deletedRelatedDocs) {
                      let sameAsRelated = true
                      for(let field of compFieldsRelDocs) {
                        if(deletedRelDoc[field] != doc2[field]) {
                          sameAsRelated = false
                          break
                        }
                      }
                      if(sameAsRelated) {
                        totalDeletedRelDocs += 1
                      }
                    }
                    // total deleted should always be less by one to all related docs so that the remaining one should only be truncated
                    if(relatedDocs.length === totalDeletedRelDocs+1) {
                      continue
                    }
                  }
                  let pushed = deleteIDs.find((id) => {
                    return id === doc2._id
                  })
                  if(same && !pushed && doc2[orderedResource.name] === record[idField]) {
                    this.deletedRelatedDocs.push(doc2)
                    deleteIDs.push(doc2._id)
                  }
                }
              }
              truncateIDs = ids.filter((id) => {
                return !deleteIDs.includes(id)
              })
              async.parallel({
                truncate: (callback) => {
                  if(truncateIDs.length === 0) {
                    return callback(null)
                  }
                  //if relatedDocs is 2 then delete
                  let body = {
                    script: {
                      lang: 'painless',
                      source: ctx
                    },
                    query: {
                      terms: {
                        _id: truncateIDs
                      }
                    },
                  };
                  let sqlquery = this.generateUpdateSQL(body, index)
                  this.executeSQL(sqlquery).then(() => {
                    return callback(null)
                  }).catch(() => {
                    return callback(null)
                  })
                },
                delete: (callback) => {
                  if(deleteIDs.length === 0) {
                    return callback(null)
                  }
                  let query = {
                    query: {
                      terms: {
                        id: deleteIDs
                      }
                    }
                  }
                  let sqlquery = this.generateDeleteSQL(query, index)
                  this.executeSQL(sqlquery).then(() => {
                    return callback(null)
                  }).catch((err) => {
                    logger.error(err);
                    return callback(null)
                  })
                }
              }, () => {
                return callback(null)
              })
            }).catch((err) => {
              logger.error(err);
              return callback(null);
            })
          } else {
            return callback(null)
          }
        })
      }
    }, () => {
      return callback()
    })
  }

  cache() {
    return new Promise(async(resolve) => {
      await this.createSyncDataTable()
      this.getReportRelationship((err, relationships) => {
        if (err) {
          return;
        }
        if ((!relationships.entry || !Array.isArray(relationships.entry)) && !relationships.resourceType === 'Bundle') {
          logger.error('invalid resource returned');
          return;
        }
        async.eachSeries(relationships.entry, (relationship, nxtRelationship) => {
          logger.info('processing relationship ID ' + relationship.resource.id);
          relationship = relationship.resource;
          let details = relationship.extension.find(ext => ext.url === 'http://ihris.org/fhir/StructureDefinition/iHRISReportDetails');
          let links = relationship.extension.filter(ext => ext.url === 'http://ihris.org/fhir/StructureDefinition/iHRISReportLink');
          let reportDetails = this.flattenComplex(details.extension);
          if(reportDetails.cachingDisabled === true) {
            return nxtRelationship()
          }
          let IDFields = [];
          for (let linkIndex1 in links) {
            let link1 = links[linkIndex1];
            let flattenedLink1 = this.flattenComplex(link1.extension);
            let linkTo1 = flattenedLink1.linkTo.split('.')
            let linkToResource1 = linkTo1[0]
            if (linkToResource1 === reportDetails.name) {
              let fhirpath
              if (linkTo1.length === 1) {
                fhirpath = 'id'
              } else {
                linkTo1.splice(0, 1)
                fhirpath = linkTo1.join('.')
              }
              details.extension.push({
                url: 'http://ihris.org/fhir/StructureDefinition/iHRISReportElement',
                extension: [{
                    url: 'name',
                    valueString: '__' + flattenedLink1.name + '_link',
                  },
                  {
                    url: 'fhirpath',
                    valueString: fhirpath,
                  },
                  {
                    url: 'autoGenerated',
                    valueBoolean: true
                  }
                ],
              });
              IDFields.push({
                name: '__' + flattenedLink1.name + '_link',
                type: "string"
              })
            }

            IDFields.push({
              name: flattenedLink1.name,
              type: "string"
            });
            for (let link2 of links) {
              let flattenedLink2 = this.flattenComplex(link2.extension);
              let linkTo2 = flattenedLink2.linkTo.split('.')
              let linkToResource2 = linkTo2[0]
              if (linkToResource2 === flattenedLink1.name) {
                let fhirpath
                if (linkTo2.length === 1) {
                  fhirpath = 'id'
                } else {
                  linkTo2.splice(0, 1)
                  fhirpath = linkTo2.join('.')
                }
                links[linkIndex1].extension.push({
                  url: 'http://ihris.org/fhir/StructureDefinition/iHRISReportElement',
                  extension: [{
                      url: 'name',
                      valueString: '__' + flattenedLink2.name + '_link',
                    },
                    {
                      url: 'fhirpath',
                      valueString: fhirpath,
                    },
                    {
                      url: 'autoGenerated',
                      valueBoolean: true
                    }
                  ],
                });
                IDFields.push({
                  name: '__' + flattenedLink2.name + '_link',
                  type: "string"
                })
              }
            }
          }
          reportDetails = this.flattenComplex(details.extension);
          this.orderedResources = [];
          // reportDetails.resource = subject._type;
          this.orderedResources.push(reportDetails);
          IDFields.push({
            name: reportDetails.name,
            type: "string"
          });
          this.getLastIndexingTime(reportDetails.name).then(() => {
            let newlast_began_indexing_time = moment().format('Y-MM-DDTHH:mm:ssZ');
            this.createTable(reportDetails.name, IDFields, relationship).then(() => {
              logger.info('Done creating table');
              this.getImmediateLinks(links, () => {
                async.eachSeries(this.orderedResources, (orderedResource, nxtResourceType) => {
                  (async () => {
                    let url = URI(this.FHIRBaseURL).segment(orderedResource.resource)
                    if(orderedResource.initialFilter && (this.reset || this.last_began_indexing_time === '1970-01-01T00:00:00')) {
                      let initFilter = orderedResource.initialFilter.split("=")
                      url.addQuery(initFilter[0], initFilter[1])
                    }
                    url.addQuery('_count', -1)
                    .addQuery('_total', 'accurate')
                    url = url.toString()
                    await axios.get(url).then((resp) => {
                      this.totalResources = resp.data.total
                    })
                  })()
                  this.deletedRelatedDocs = []
                  let processedRecords = []
                  this.count = 1;
                  let offset = 0
                  let url = URI(this.FHIRBaseURL).segment(orderedResource.resource)
                  if(!this.reset && this.last_began_indexing_time !== '1970-01-01T00:00:00') {
                    url.segment('_history')
                  }
                  if(orderedResource.initialFilter && (this.reset || this.last_began_indexing_time === '1970-01-01T00:00:00')) {
                    let initFilter = orderedResource.initialFilter.split("=")
                    url.addQuery(initFilter[0], initFilter[1])
                  }
                  url.addQuery('_since', this.last_began_indexing_time)
                    .addQuery('_count', 200)
                  url = url.toString();
                  logger.info(`Processing data for resource ${orderedResource.name}`);
                  async.whilst(
                    callback => {
                      return callback(null, url != false);
                    },
                    callback => {
                      axios.get(url, {
                        headers: {
                          'Cache-Control': 'no-cache',
                        },
                        withCredentials: true,
                        auth: {
                          username: this.FHIRUsername,
                          password: this.FHIRPassword,
                        },
                      }).then(response => {
                        url = false;
                        const next = response.data.link.find(
                          link => link.relation === 'next'
                        );
                        if (next) {
                          url = next.url
                        }
                        if (response.data.entry && response.data.entry.length > 0) {
                          //if hapi server doesnt have support for returning the next cursor then use _getpagesoffset
                          offset += 200
                          if(offset <= this.totalResources && !url) {
                            url = URI(this.FHIRBaseURL).segment(orderedResource.resource)
                            if(!this.reset && this.last_began_indexing_time !== '1970-01-01T00:00:00') {
                              url.segment('_history')
                            }
                            url.addQuery('_since', this.last_began_indexing_time)
                            .addQuery('_count', 200)
                            .addQuery('_getpagesoffset', offset)
                            url = url.toString();
                          }
                          this.processResource(response.data.entry, orderedResource, reportDetails, processedRecords, false, () => {
                            return callback(null, url);
                          })
                        } else {
                          if(response.data.type) {
                            url = false
                            return callback(null, url);
                          } else {
                            return callback(null, url);
                          }
                        }
                      }).catch(err => {
                        // Handle expired cursor
                        if(err && err.response && err.response.status === 410) {
                          let newurl = URI(url)
                          let queries = newurl.query().split('&')
                          let pageoffset
                          let count
                          for(let query of queries) {
                            let qr = query.split('=')
                            if(qr[0] === '_getpagesoffset') {
                              pageoffset = qr[1]
                            } else if(qr[0] === '_count') {
                              count = qr[1]
                            }
                          }
                          if(!pageoffset) {
                            logger.error('Error occured while getting resource data');
                            logger.error(err);
                            return callback(null, false)
                          }
                          url = URI(this.FHIRBaseURL)
                          .segment(orderedResource.resource)
                          .segment('_history')
                          .addQuery('_since', this.last_began_indexing_time)
                          .addQuery('_count', count)
                          .addQuery('_getpagesoffset', pageoffset)
                          .toString();
                          return callback(null, url)
                        } else {
                          logger.error('Error occured while getting resource data');
                          logger.error(err);
                          return callback(null, false)
                        }
                      });
                    }, async() => {
                      logger.info('Done Writting resource data for resource ' + orderedResource.name + ' into elastic search');
                      this.fixDataInconsistency(reportDetails, orderedResource, () => {
                        return nxtResourceType()
                      })
                    }
                  );
                }, () => {
                  try {
                    let newlast_ended_indexing_time = moment().format('Y-MM-DDTHH:mm:ssZ');
                    this.updateLastIndexingTime(newlast_began_indexing_time, newlast_ended_indexing_time, reportDetails.name)
                  } catch (error) {
                    logger.error(error);
                  }
                  return nxtRelationship();
                });
              });
            }).catch(() => {
              logger.error('Stop creating report due to error in creating index');
              return nxtRelationship();
            });
          }).catch((err) => {
            logger.error(err);
            return nxtRelationship();
          })
        }, () => {
          logger.info('Done processing all relationships');
          return resolve()
        });
      });
    })
  }

  processResource(resourceData, orderedResource, reportDetails, processedRecords, wait, callback) {
    async.eachSeries(resourceData, (data, nxtResource) => {
      logger.info('processing ' + this.count + '/' + this.totalResources + ' records of resource ' + orderedResource.resource);
      this.count++
      let deleteRecord = false;
      if (!data.resource || !data.resource.resourceType) {
        if(data.request && data.request.method === 'DELETE') {
          deleteRecord = true
        } else {
          return nxtResource()
        }
      }

      let id
      if(data.resource && data.resource.id) {
        id = orderedResource.resource + '/' + data.resource.id;
      } else if(data.request && data.request.url) {
        let urlArr = data.request.url.split(orderedResource.resource)
        let resId = urlArr[1].split('/')[1]
        id = orderedResource.resource + '/' + resId;
      } else {
        logger.error('Invalid FHIR data returned');
        return nxtResource()
      }
      let processed
      for(let k=0;k<processedRecords.length;k++) {
        if(processedRecords[k] === id) {
          processed = processedRecords[k]
          break
        }
      }
      if (!processed) {
        processedRecords.push(id)
      } else {
        return nxtResource()
      }
      let getDeletedResProm = new Promise((resolve) => {
        //if deleted resource is not the primary resource then get the resource in the state before deleted then truncate elasticsearch based on that state
        if(data.request && data.request.method === 'DELETE') {
          let resVersion = data.request.url.split('/').pop()
          let oldUrlArr = data.request.url.split('/')
          oldUrlArr[oldUrlArr.length - 1] = parseInt(resVersion) - 1
          let url = oldUrlArr.join('/')
          axios.get(url, {
            headers: {
              'Cache-Control': 'no-cache',
            },
            withCredentials: true,
            auth: {
              username: this.FHIRUsername,
              password: this.FHIRPassword,
            },
          }).then(response => {
            data.resource = response.data
            return resolve()
          }).catch((err) =>{
            logger.error('Error occured while getting deleted resource');
            logger.error(err);
            return resolve()
          })
        } else {
          return resolve()
        }
      })
      getDeletedResProm.then(async() => {
        if (orderedResource.query) {
          try {
            let queryResult = FHIRPath.evaluate(data.resource, orderedResource.query);
            if((Array.isArray(queryResult) && (queryResult.includes(false) || queryResult.length === 0)) || queryResult === false) {
              deleteRecord = true
            }
          } catch (error) {
            logger.error(`Invalid fhirpath supplied ${orderedResource.query}`)
            process.exit()
          }
        }
        //if this resource doesnt meet filter, and no changes made to it, then ignore processing it.
        if((deleteRecord && data.resource.meta && data.resource.meta.versionId == '1') || (deleteRecord && this.last_began_indexing_time === '1970-01-01T00:00:00')) {
          return nxtResource();
        }
        let match = {};
        if (orderedResource.hasOwnProperty('linkElement')) {
          let linkElement = orderedResource.linkElement.replace(orderedResource.resource + '.', '');
          let linkTo
          try {
            linkTo = FHIRPath.evaluate(data.resource, linkElement);
          } catch (error) {
            logger.error(`Invalid fhirpath supplied ${linkElement}`);
            process.exit()
          }
          if (linkElement === 'id') {
            linkTo = orderedResource.resource + '/' + linkTo
          }
          if(!Array.isArray(linkTo)) {
            if(!linkTo) {
              linkTo = []
            } else {
              linkTo = [linkTo]
            }
          }
          match['__' + orderedResource.name + '_link'] = linkTo;
        } else {
          match[orderedResource.name] = [data.resource.resourceType + '/' + data.resource.id];
        }
        let record = {};
        if(orderedResource["http://ihris.org/fhir/StructureDefinition/iHRISReportElement"]) {
          for (let element of orderedResource["http://ihris.org/fhir/StructureDefinition/iHRISReportElement"]) {
            let fieldName
            let fhirpath
            let externalfunction
            let displayformat
            let fieldAutogenerated = false
            let valueModifier
            for (let el of element) {
              let value = '';
              for (let key of Object.keys(el)) {
                if (key !== 'url') {
                  value = el[key];
                }
              }
              if (el.url === "name") {
                let fleldChars = value.split(' ')
                //if name has space then format it
                if (fleldChars.length > 1) {
                  fieldName = value.toLowerCase().split(' ').map(word => word.replace(word[0], word[0].toUpperCase())).join('');
                } else {
                  fieldName = value
                }
              } else if (el.url === "fhirpath") {
                fhirpath = value
              } else if (el.url === "function") {
                externalfunction = value
              } else if (el.url === "displayformat") {
                displayformat = value
              } else if (el.url === "autoGenerated") {
                fieldAutogenerated = value
              } else if(el.url === 'valueModifier') {
                valueModifier = value
              }
            }
            let displayData
            try {
              if(fhirpath) {
                if(fhirpath.startsWith('concat(')) {
                  fhirpath = fhirpath.substring(7, fhirpath.length-1)
                }
                let fhirpathes = fhirpath.split(',')
                for(let path of fhirpathes) {
                  let value
                  try {
                    value = FHIRPath.evaluate(data.resource, path);
                  } catch (error) {
                    logger.error(`Invalid fhirpath supplied ${path}`)
                    process.exit()
                  }
                  if(Array.isArray(value)) {
                    for(let val of value) {
                      if(val.reference) {
                        let displayFormat = element.find((el) => {
                          return el.url === 'displayformat'
                        })
                        let refResource = await this.getResourceFromReference(val.reference)
                        value = await this.resourceDisplayName(refResource, displayFormat)
                        value = [value]
                      }
                    }
                    value = value.join(',')
                  }
                  if(!displayData) {
                    displayData = value
                  } else {
                    displayData += ' ' + value
                  }
                }
              } else if(externalfunction) {
                let fncn = externalfunction.split(".")
                if(fncn.length !== 2) {

                }
                let functionname = fncn[1]
                let params = this.getExtFuncParams(functionname)
                functionname = functionname.split("(")[0]
                let currentrow = {}
                //check if all params are available in record, otherwise fetch records from ES
                let missing = false
                if(params) {
                  params = params.split(",")
                  for(let param of params) {
                    param = param.trim()
                    if(!record.hasOwnProperty(param)) {
                      missing = true
                      break
                    }
                  }
                }
                if(missing) {
                  //get row data
                  let query = {
                    query: {
                      terms: match
                    }
                  }
                  let sqlquery = this.generateSelectSQL(query, reportDetails.name)
                  await this.executeSQL(sqlquery, 'SELECT').then((response) => {
                    if(response && Array.isArray(response) && response.length) {
                      currentrow = response[0]
                      currentrow = _.merge(currentrow, record)
                    }
                  }).catch(() => {
                    return callback(null)
                  })
                } else {
                  currentrow = record
                }
                try {
                  externalfunction = require(this.ESModulesBasePath + "/" + fncn[0])
                } catch (error) {
                  logger.error(error);
                }
                try {
                  displayData = await externalfunction[functionname](currentrow).catch((err) => {
                    logger.error(err);
                  })
                } catch (error) {
                  logger.error(error);
                }
              } else {
                let displayFormat = element.find((el) => {
                  return el.url === 'displayformat'
                })
                let value = await this.resourceDisplayName(data.resource, displayFormat)
                if(!displayData) {
                  displayData = value
                } else {
                  displayData += ' ' + value
                }
              }
            } catch (error) {
              logger.error(error);
            }
            let value
            if ((!displayData || (Array.isArray(displayData) && displayData.length === 1 && displayData[0] === undefined)) && data.resource.extension) {
              try {
                value = await this.getElementValFromExtension(data.resource.extension, fhirpath)
              } catch (error) {
                logger.error(error);
              }
            } else if (Array.isArray(displayData) && displayData.length === 1 && displayData[0] === undefined) {
              value = undefined
            } else if (Array.isArray(displayData)) {
              if(fieldName.startsWith('__') && orderedResource.multiple) {
                // value = displayData
                value = displayData.pop();
              } else {
                value = displayData.pop();
              }
            } else {
              value = displayData;
            }
            if (value || value === 0 || value === false) {
              if (typeof value == 'object') {
                if (value.reference && fieldAutogenerated) {
                  value = value.reference
                } else if (value.reference && !fieldAutogenerated) {
                  let referencedResource
                  try {
                    referencedResource = await this.getResourceFromReference(value.reference);
                  } catch (error) {
                    logger.error(error);
                  }
                  if (referencedResource) {
                    value = referencedResource.name
                  }
                } else {
                  value = JSON.stringify(value)
                }
              }
              if (fhirpath === 'id') {
                value = data.resource.resourceType + '/' + value
              }
              if(valueModifier) {
                let modVal = this.modifyValue(value, valueModifier)
                if(modVal) {
                  value = modVal
                }
              }
              record[fieldName] = value
            } else {
              record[fieldName] = null
            }
          }
        }
        record[orderedResource.name] = id
        let ctx = '';
        for (let field in record) {
          // cleaning to escape ' char
          if (record[field] && typeof record[field] === 'string') {
            let recordFieldArr = record[field].split('')
            for (let recordFieldIndex in recordFieldArr) {
              let char = recordFieldArr[recordFieldIndex]
              if (char === "'") {
                recordFieldArr[recordFieldIndex] = "\\'"
              }
            }
            record[field] = recordFieldArr.join('');
          }
          if(deleteRecord || !record[field]) {
            ctx += `ctx._source['${field}']=null;`;
            if(field.startsWith('__')) {
              let truncateResources = this.getChildrenResources(orderedResource.name)
              let truncateFields = this.getResourcesFields(truncateResources)
              for(let truncField of truncateFields) {
                ctx += `ctx._source['${truncField.field}']=null;`;
              }
            }
          } else {
            ctx += `ctx._source['${field}']='${record[field]}';`;
          }
        }
        // truncate fields of any other resources that are linked to this resource
        if(deleteRecord) {
          let childrenResources = this.getChildrenResources(orderedResource.name);
          let fields = this.getResourcesFields(childrenResources)
          for(let field of fields) {
            ctx += `ctx._source['${field.field}']=null;`;
          }
        }

        let body = {
          script: {
            lang: 'painless',
            source: ctx
          },
          query: {
            terms: match ,
          },
        };
        // check if linkTo (__linkname_link) has a cardinality of 1..* and dupplicate the request per linkTo
        let dupBasedOnfields = []
        for(let rec in record) {
          if(rec.startsWith('__') && rec.endsWith('_link') && record[rec] && record[rec].split(',').length > 1) {
            dupBasedOnfields.push(rec)
          }
        }
        let modifiedRecAndBody = []
        if(dupBasedOnfields.length > 0) {
          for(let index in dupBasedOnfields) {
            let dupBasedOnfield = dupBasedOnfields[index]
            if(modifiedRecAndBody.length > 0) {
              let partialMods = []
              for(let modIndex in modifiedRecAndBody) {
                modified = modifiedRecAndBody[modIndex]
                let partialMod = this.dupplicateRequest(modified.record, modified.body, dupBasedOnfield)
                partialMods = partialMods.concat(partialMod)
              }
              modifiedRecAndBody = partialMods
            } else {
              modifiedRecAndBody = this.dupplicateRequest(record, body, dupBasedOnfield)
            }
          }
        } else {
          modifiedRecAndBody = [{
            record,
            body
          }]
        }
        async.eachSeries(modifiedRecAndBody, (modified, nxtMod) => {
          let extraTerms = []
          if(dupBasedOnfields.length > 0) {
            for(let dupBasedOnfield of dupBasedOnfields) {
              let tmpTerm = {
                terms: {}
              }
              tmpTerm.terms[dupBasedOnfield] = [modified.record[dupBasedOnfield]]
              extraTerms.push(tmpTerm)
            }
          }
          if(!deleteRecord) {
            this.updateESDocument(modified.body, modified.record, reportDetails.name, orderedResource, data.resource, deleteRecord, extraTerms, () => {
              //if this resource supports multiple rows i.e Group linked to Practitioner, then cache resources in series
              if(orderedResource.multiple || wait) {
                return nxtMod()
              }
            })
            if(!orderedResource.multiple && !wait) {
              return nxtMod()
            }
          } else {
            //if this is the primary resource then delete the whole document, otherwise delete respective fields data
            if(!orderedResource.hasOwnProperty('linkElement')) {
              let query = {
                query: {
                  bool: {
                    must: [{
                      terms: modified.body.query.terms
                    }]
                  }
                }
              }
              if(extraTerms.length > 0) {
                query.query.bool.must = query.query.bool.must.concat(extraTerms)
              }
              let sqlquery = this.generateDeleteSQL(query, reportDetails.name)
              this.executeSQL(sqlquery).then(() => {
                return nxtMod()
              }).catch((err) => {
                logger.error(err);
                return nxtMod()
              })
            } else {
              this.updateESDocument(modified.body, modified.record, reportDetails.name, orderedResource, data.resource, deleteRecord, extraTerms, () => {
                //if this resource supports multiple rows i.e Group linked to Practitioner, then cache resources in series
                if(orderedResource.multiple || wait) {
                  return nxtMod()
                }
              })
              if(!orderedResource.multiple && !wait) {
                return nxtMod()
              }
            }
          }
        }, () => {
          return nxtResource();
        })
      }).catch((err) => {
        logger.error(err);
        return nxtResource();
      })
    }, () => {
      return callback()
    });
  }

  getExtFuncParams(fn) {
    //get anything inside({})
    let regex = /\{([^}]*)\}/;
    let match = fn.match(regex);
    //if not, try anything inside ()
    if(!match){
      regex = /\(([^)]*)\)/;
      match = fn.match(regex);
    }
    if (match) {
        return match[1];
    } else {
        return "";
    }
  }

  fixDataInconsistency(reportDetails, orderedResource, callback) {
    logger.info("Fixing data inconsistency for resource " + orderedResource.resource)
    //these must be run in series
    let fieldStillMissing = false
    async.series({
      /**
       * this fix missing data i.e __location_link is available but location is missing
       * Example: PractitionerRole didn't have a location, then later on a location get assigned to this PractitionerRole,
       * Location resource will not be pulled as it was not changed, it is the PractitionerRole alone that was updated, this piece of code will check and force an update.
       */
      fixMissing: (callback) => {
        //ignore reversed linked resources as it doesnt apply for them i.e role linked to practitioner, it doesnt ignore when practitioner is linked to role
        if((orderedResource.linkTo && orderedResource.linkTo.split('.').length === 1) || (orderedResource.linkTo && orderedResource.linkTo.split('.').length === 2 && orderedResource.linkTo.split('.')[1] === 'id')) {
          return callback(null)
        }
        let sql = `select * from ${pluralize(reportDetails.name)} where (${this.formatColumn(orderedResource.name)} is NULL or ${this.formatColumn(orderedResource.name)}='') and lastUpdated>'${this.last_ended_indexing_time}'`
        runCleaner(sql, false, this, (missing) => {
          if(missing) {
            fieldStillMissing = true
          }
          return callback(null)
        })
      },
      //this fix invalid data i.e __location_link not equal to location, and location is always invalid, not __location_link
      differences: (callback) => {
        //ignore reversed linked resources as it doesnt apply for them i.e role linked to practitioner, it doesnt ignore when practitioner is linked to role
        if((orderedResource.linkTo && orderedResource.linkTo.split('.').length === 1) || (orderedResource.linkTo && orderedResource.linkTo.split('.').length === 2 && orderedResource.linkTo.split('.')[1] === 'id')) {
          return callback(null)
        }
        if(!orderedResource.linkElement || fieldStillMissing) {
          return callback(null)
        }
        let sql = `select * from ${pluralize(reportDetails.name)} where ((__${orderedResource.name}_link is NOT NULL and __${orderedResource.name}_link !='') and (${this.formatColumn(orderedResource.name)} is NOT NULL and ${this.formatColumn(orderedResource.name)}!='')) and (__${orderedResource.name}_link != ${this.formatColumn(orderedResource.name)})`
        runCleaner(sql, true, this, () => {
          return callback(null)
        })
      }
    }, () => {
      return callback()
    })

    function runCleaner(query, ignoreReverseLinked, me, callback) {
      try {
        me.executeSQL(query, 'SELECT').then((results) => {
          if(results.length === 0) {
            return callback(null)
          }
          /**
           * this is for reversed linked resources i.e Practitioner and PractitionerRole
           */
          let reverseLink = false
          // end of reversed linked resources
          let resIds = []
          async.eachOfSeries(results, (doc, index, nxtDoc) => {
            if(doc['__' + orderedResource.name + '_link']) {
              if(resIds.length === 0) {
                let linkResType = doc['__' + orderedResource.name + '_link'].split('/')[0]
                if(linkResType !== orderedResource.resource) {
                  reverseLink = true
                  if(ignoreReverseLinked) {
                    return nxtDoc();
                  }
                }
              }
              resIds.push(doc['__' + orderedResource.name + '_link'])
            } else {
              //logger.error(JSON.stringify(doc,0,2));
              //logger.error('There is a serious data inconsistency that needs to be addressed on index ' + reportDetails.name + ' and index id ' + doc._id + ', field ' + '__' + orderedResource.name + '_link' + ' is missing');
              return nxtDoc();
            }
            let ids
            if(resIds.length === 100 || index === (results.length - 1)) {
              ids = resIds.join(',')
              resIds = []
            } else {
              return nxtDoc()
            }

            if(!ids || ids == 'null' || (ignoreReverseLinked && reverseLink)) {
              return nxtDoc()
            }
            let processedRecords = []
            let url = URI(me.FHIRBaseURL)
              .segment(orderedResource.resource)
              .addQuery('_count', 200)
            if(!reverseLink) {
              url = url.addQuery('_id', ids)
            } else {
              if(!orderedResource.linkElementSearchParameter) {
                logger.error('linkElementSearchParameter is missing, cant fix data inconsistency');
                return callback()
              }
              url = url.addQuery(orderedResource.linkElementSearchParameter, ids)
            }
            url = url.toString()
            async.whilst(
              (callback) => {
                return callback(null, url !== null)
              },
              (callback) => {
                axios.get(url, {
                  headers: {
                    'Cache-Control': 'no-cache',
                  },
                  withCredentials: true,
                  auth: {
                    username: me.FHIRUsername,
                    password: me.FHIRPassword,
                  },
                }).then(response => {
                  me.totalResources = response.data.total;
                  url = null;
                  const next = response.data.link.find(
                    link => link.relation === 'next'
                  );
                  if (next) {
                    url = next.url
                  }
                  if (response.data.entry && response.data.entry.length > 0) {
                    me.count = 1;
                    me.processResource(response.data.entry, orderedResource, reportDetails, processedRecords, false, () => {
                      return callback(null, url);
                    })
                  } else {
                    return callback(null, url);
                  }
                }).catch((err) => {
                  logger.error('Error occured while getting resource data');
                  logger.error(err);
                  return callback(null, null)
                })
              },
              () => {
                return nxtDoc()
              }
            )
          }, async() => {
            return callback()
          })
        })
      } catch (error) {
        logger.error(error);
        return callback()
      }
    }
  }

  modifyValue(value, modifier) {
    // let modifier = 'send_status==completed||send_status==wow?Sent:send_status==entered-in-error?Failed:Unknown'
    let steps = modifier.split(':')
    let modifiedValue = ''
    for(let step of steps) {
      let stepValueArr = step.split('?')
      if(stepValueArr.length === 1) {
        modifiedValue = step
        break
      }
      let stepValue = step.split('?')[1]
      step = step.split('?')
      step.pop()
      step = step.join()
      let opts = step.split('||')
      for(let opt of opts) {
        opt = opt.replace('===', '=')
        opt = opt.replace('==', '=')
        opt = opt.replace('!==', '!=')
        if(opt.includes('=') && value == opt.split('=')[1]) {
          modifiedValue = stepValue
          break;
        } else if(opt.includes('!=') && value != opt.split('=')[1]) {
          modifiedValue = stepValue
          break;
        }
      }
      if(modifiedValue) {
        break
      }
    }
    return modifiedValue
  }

  dupplicateRequest(record, body, dupBasedOnfield) {
    let modifiedData = []
    let links = record[dupBasedOnfield].split(',')
    for(let link of links) {
      let newRecord = _.cloneDeep(record)
      newRecord[dupBasedOnfield] = link
      let newBody = _.cloneDeep(body)
      let source = newBody.script.source
      let sources = source.split(';')
      for(let index in sources) {
        if(sources[index].startsWith(`ctx._source['${dupBasedOnfield}']`) || sources[index].startsWith(`ctx._source["${dupBasedOnfield}"]`) || sources[index].startsWith(`ctx._source.${dupBasedOnfield}`)) {
          sources[index] = `ctx._source.${dupBasedOnfield}='${link}'`
          break
        }
      }
      newBody.script.source = sources.join(';')
      modifiedData.push({
        record: newRecord,
        body: newBody
      })
    }
    return modifiedData
  }

  generateId(ids) {
    if(!ids || !Array.isArray(ids) || ids.length === 0) {
      return ids
    }
    if(ids.length < 2) {
      return ids[0]
    }
    return uuid5(ids.join(''), '8f84db7c-93f9-4800-8aa2-1c8913a1dc54')
  }


}
module.exports = {
  CacheFhirToES
}
