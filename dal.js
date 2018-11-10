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
            database.collection('coins').createIndexes([{ key: {address: 1}, name: "idx_addr" }, { key: {tx_id: 1, pos: 1}, name: "idx_xo"}]), 

            database.collection('payloads').createIndexes([{ key: { address: 1, hint: 1 }, name: "idx_addr_hint" }]),

            database.collection('pending_spents').createIndexes([{ key: { address: 1 }, name: "idx_addr" }, { key: {tx_id: 1}, name: "idx_tx" } ]),
            database.collection('pending_coins').createIndexes([{ key: { address: 1 }, name: "idx_addr" }, { key: {tx_id: 1}, name: "idx_tx" } ]),
            database.collection('pending_payloads').createIndexes([{ key: {address: 1}, name: "idx_addr" }, { key: {tx_id: 1}, name: "idx_tx"}]),

            database.collection('rejects').createIndexes([{ key: { tx_id: 1 }, name: "idx_tx" }])
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

    async addErrors(errs){
        return database.collection("errors").insertMany(errs);
    },

    async addCoins(coins){
        return database.collection("coins").insertMany(coins);
    },

    async addPayloads(payloads){
        return database.collection("payloads").insertMany(payloads);
    },

    async addSpents(spents){
        return Promise.all(spents.map(spent => {
            return database.collection("coins").remove(
                { tx_id: spent.spent_tx_id, pos: spent.pos }
            );
            /*
            return database.collection("coins").findOneAndUpdate(
                { tx_id: spent.tx_id, pos: spent.pos },
                { $set: { spent: true }},
                { upsert: false }
            );
            */
        }));
    },

    async addPendingSpents(spents){
        return database.collection("pending_spents").insertMany(spents);
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
            return Promise.all([
                database.collection("pending_coins").remove(filter),
                database.collection("pending_payloads").remove(filter),
                database.collection("pending_spents").remove(filter)
            ]);
        }));
    },

    async check_rejection(){
        let spents = await database.collection("pending_spents").find().toArray();
        if(spents.length > 0){
            let rejects = new Set();

            await Promise.all(spents.map(sp => {
                if(!rejects.has(sp.tx_id)){
                    return database.collection("coins").count({tx_id: {$eq: sp.spent_tx_id}, pos: {$eq: sp.pos}}).then(count => {
                        if(count == 0){
                            //spent missing, must have been consumed by another transaction on blockchain!
                            rejects.add(sp.tx_id);
                        }
                    })
                }
            }));

            if(rejects.size > 0){
                let txids = Array.from(rejects);
                await Promise.all([
                    database.collection("rejects").insertMany(txids.map(x => {tx_id: x})),
                    this.removePendingTransactions(txids)
                ])
            }
        }
    }
}