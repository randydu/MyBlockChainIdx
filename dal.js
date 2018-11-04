'use strict'

/**
 * Data Access Layer
 */

const common = require('./common');
const debug = common.create_debug('dal');
const config = common.config;

const mongodb = config.mongodb;
debug.info("%O", mongodb);

const MongoClient = require('mongodb').MongoClient;
var mydb = null; //db connection
var database = null; 

module.exports = {
    async init(){
        debug.info("dal.init >>");

        return MongoClient.connect(mongodb.url).then( db => {
            mydb = db;
            database = mydb.db('myidx');

            return database.collection('coins').createIndex({
                address: 1
            });

            debug.trace("%O", database);
            debug.info("dal.init <<");
        }).catch(err => {
            debug.err(err.message);
            throw err;
        })
    },

    close(){
        if(mydb) mydb.close();
    },

    setLastRecordedBlockHeight(height){
        return database.collection("summary").replaceOne({ field: 'lastBlockHeight' }, {
            field: 'lastBlockHeight',
            value: height
        }, { upsert: true });
    },

    getLastRecordedBlockHeight(){
        return database.collection("summary").findOne({ field: 'lastBlockHeight'}).then( r => r.value );
    },

    addCoins(coins){
        return database.collection("coins").insertMany(coins);
    },

    addPayloads(payloads){
        return database.collection("payloads").insertMany(payloads);
    },

}