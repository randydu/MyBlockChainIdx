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

        mydb = await MongoClient.connect(mongodb.url);
        database = mydb.db('myidx');

        await database.collection('coins').createIndex({ address: 1 });
        await database.collection('payloads').createIndex({ address: 1 });

        debug.info("dal.init <<");
    },

    close(){
        if(mydb) mydb.close();
    },

    async setLastRecordedBlockHeight(height){
        return database.collection("summary").replaceOne({ field: 'lastBlockHeight' }, {
            field: 'lastBlockHeight',
            value: height
        }, { upsert: true });
    },

    async getLastRecordedBlockHeight(){
        let r = await database.collection("summary").findOne({ field: 'lastBlockHeight'});
        return r == null ? -1 : r.value;
    },

    async addCoins(coins){
        return database.collection("coins").insertMany(coins);
    },

    async addPayloads(payloads){
        return database.collection("payloads").insertMany(payloads);
    },

    async addSpents(spents){
        return Promise.all(spents.map(spent => {
            return database.collection("coins").findOneAndUpdate(
                { tx_id: spent.tx_id, pos: spent.pos },
                { $set: { spent: true }},
                { upsert: false }
            );
        }));
    }

}