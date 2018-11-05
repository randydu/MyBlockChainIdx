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

        await Promise.all([
            database.collection('coins').createIndexes([{ key: {address: 1}, name: "idx_addr" }, { key: {tx_id: 1, pos: 1}, name: "idx_spent"}]), 

            database.collection('payloads').createIndexes([{ key: { address: 1 }, name: "idx_addr" }]),

            database.collection('pending_coins').createIndexes([{ key: { address: 1 }, name: "idx_addr" }, { key: {tx_id: 1}, name: "idx_tx" } ]),
            database.collection('pending_payloads').createIndexes([{ key: {address: 1}, name: "idx_addr" }, { key: {tx_id: 1}, name: "idx_tx"}])
        ]);

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
    },

    async addPendingCoins(coins){
        return database.collection("pending_coins").insertMany(coins);
    },

    async addPendingPayloads(payloads){
        return database.collection("pending_payloads").insertMany(payloads);
    },

    async removePendingTransactions(txids){
        return Promise.all(txids.map(txid => {
            let filter = { tx_id: { $eq: txid }};
            return database.collection("pending_coins").remove(filter).then(()=> {
                return database.collection("pending_payloads").remove(filter);
            })
        }));
    },
}