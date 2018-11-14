'use strict'

/**
 * Data Access Layer
 */

const common = require('./common');
const debug = common.create_debug('dal');
const config = common.config;


const MongoClient = require('mongodb').MongoClient;
var client = null;
var database = null; 

async function getNextCoinId(){
    let r = await database.collection("coins").find().sort({_id: -1}).limit(1).next();
    return r == null ? 1 : r._id + 1;
}

async function getNextMultiSigCoinId(){
    let r = await database.collection("coins_multisig").find().sort({_id: -1}).limit(1).next();
    return r == null ? 1 : r._id + 1;
}

module.exports = {
    async init(){
        debug.info("dal.init >>");

        let mongodb_url = process.env.MONGODB_URL;
        debug.info("MONGODB_URL=%s", mongodb_url);
        client = await MongoClient.connect(mongodb_url, { useNewUrlParser: true });
        database = client.db('myidx');

        const support_payload = config.coin_traits.payload;
        const support_multisig = config.coin_traits.MULTISIG;

        await Promise.all([
            database.createCollection("coins"),
            support_multisig ? database.createCollection("coins_multisig") : Promise.resolve(),
            database.createCollection("pending_spents"),
            database.createCollection("pending_coins"),
            support_multisig ? database.createCollection("pending_coins_multisig") : Promise.resolve(),
            database.createCollection("rejects"),

            support_payload ? database.createCollection("payloads") : Promise.resolve(),
            support_payload ? database.createCollection("pending_payloads") : Promise.resolve(),
            
        ]);

        await Promise.all([
            database.collection('coins').createIndexes([
                { key: {address: 1}, name: "idx_addr" }, 
                { key: {height: 1}, name: "idx_height" }, 
                { key: {tx_id: 1, pos: 1}, name: "idx_xo", unique: config.coin_traits.BIP34 },//multiple (tx_id,pos) in coinbase pre-BIP34
            ]), 

            support_multisig ? database.collection('coins_multisig').createIndexes([
                { key: {addresses: 1}, name: "idx_addr" }, 
                { key: {height: 1}, name: "idx_height" }, 
                { key: {tx_id: 1, pos: 1}, name: "idx_xo", unique: true }, //multisig cannot appear in coinbase, so it should be unique
            ]) : Promise.resolve(), 

            support_payload ? database.collection('payloads').createIndexes([
                { key: { address: 1, hint: 1, subhint:1 }, name: "idx_addr_hint" }, 
                { key: {height: 1}, name: "idx_height" }, 
                { key: {tx_id: 1, pos: 1}, name: "idx_xo", unique: true },
            ]) : Promise.resolve(),

            database.collection('pending_spents').createIndexes([
                { key: { address: 1 }, name: "idx_addr" }, 
                { key: {tx_id: 1}, name: "idx_tx" } 
            ]),
            database.collection('pending_coins').createIndexes([
                { key: { address: 1 }, name: "idx_addr" }, 
                { key: {tx_id: 1}, name: "idx_tx" } 
            ]),
            
            support_multisig ?
                database.collection('pending_coins_multisig').createIndexes([
                    { key: { addresses: 1 }, name: "idx_addr" }, 
                    { key: {tx_id: 1}, name: "idx_tx" } 
                ]) : Promise.resolve(),
            
            support_payload ? database.collection('pending_payloads').createIndexes([
                { key: {address: 1}, name: "idx_addr" }, 
                { key: {tx_id: 1}, name: "idx_tx"}
            ]): Promise.resolve(),

            database.collection('rejects').createIndexes([
                { key: { tx_id: 1 }, name: "idx_tx", unique: true }
            ])
        ]);

        debug.info("dal.init <<");
    },

    async close(){
        if(client) await client.close();
    },

    async setLastValue(key, v){
        return database.collection("summary").findOneAndUpdate(
            { _id: key }, 
            {$set: {value: v}},
            { upsert: true }
        );
    },
    async getLastValue(key){
        let r = await database.collection("summary").findOne({ _id: key });
        return r == null ? -1 : r.value;
    },

    async getLastRecordedBlockHeight(){
        return this.getLastValue('lastBlockHeight');

    },
    async setLastRecordedBlockHeight(height){
        return this.setLastValue('lastBlockHeight', height);
    },

    async addErrors(errs){
        return database.collection("errors").insertMany(errs);
    },

    async removeCoinsAfterHeight(height){
        return database.collection("coins").deleteMany({ height: { $gt: height } });
    },
    async removePayloadsAfterHeight(height){
        return database.collection("payloads").deleteMany({ height: { $gt: height } });
    },

    async addCoins(coins){
        let N = await getNextCoinId();
        coins.forEach(x => x._id = N++);

        return database.collection("coins").insertMany(coins);
    },

    async addMultiSigCoins(coins){
        let N = await getNextMultiSigCoinId();
        coins.forEach(x => x._id = N++);

        return database.collection("coins_multisig").insertMany(coins);
    },


    async addPayloads(payloads){
        let N = await database.collection("payloads").countDocuments({}) + 1;
        payloads.forEach(x => x._id = N++);
        return database.collection("payloads").insertMany(payloads);
    },

    async addSpents(spents){
        let ops = spents.map(spent => {
            return {
                deleteOne: { "filter": {"tx_id": spent.spent_tx_id, "pos": spent.pos}}
            }
        });

        await database.collection("coins").bulkWrite( ops, { ordered: false });

        if(config.coin_traits.MULTISIG){
            await database.collection("coins_multisig").bulkWrite( ops, { ordered: false });
        }
        /*
        return Promise.all(spents.map(spent => {
            //BIP34: the first coin (tx_id, pos) is spent. 
            //for non-BIP34 compatible coin, there could be multiple coins (tx_id, pos).
            //Ex: bitcoin (d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599, 0) appears in
            //block #91842, #91812
            return database.collection("coins").deleteOne(
                { tx_id: spent.spent_tx_id, pos: spent.pos }
            );
        }));
        */
    },

    async addPendingSpents(spents){
        return database.collection("pending_spents").insertMany(spents);
    },
    async addPendingCoins(coins){
        return database.collection("pending_coins").insertMany(coins);
    },
    async addPendingMultiSigCoins(coins){
        return database.collection("pending_coins_multisig").insertMany(coins);
    },

    async addPendingPayloads(payloads){
        return database.collection("pending_payloads").insertMany(payloads);
    },

    async removePendingTransactions(txids){
        let ops = txids.map(txid => {
            return {
                deleteMany: { "filter": { "tx_id": { $eq: txid } }}
            }
        });

        return Promise.all([
            database.collection("pending_coins").bulkWrite(ops, { ordered: false} ), 

            config.coin_traits.MULTISIG ? 
                database.collection("pending_coins_multisig").bulkWrite(ops, { ordered: false} ) : Promise.resolve(),

            database.collection("pending_payloads").bulkWrite( ops, { ordered: false} ),
            database.collection("pending_spents").bulkWrite(ops, { ordered: false} ),
        ]);
        
        /*
        return Promise.all(txids.map(txid => {
            let filter = { tx_id: { $eq: txid }};
            return Promise.all([
                database.collection("pending_coins").deleteMany(filter),
                database.collection("pending_payloads").deleteMany(filter),
                database.collection("pending_spents").deleteMany(filter)
            ]);
        }));
        */
    },

    async check_rejection(){
        let spents = await database.collection("pending_spents").find().toArray();
        if(spents.length > 0){
            let rejects = new Set();

            await Promise.all(spents.map(sp => {
                if(!rejects.has(sp.tx_id)){
                    return database.collection("coins").countDocuments({tx_id: {$eq: sp.spent_tx_id}, pos: {$eq: sp.pos}}).then(count => {
                        if(count == 0){
                            if(config.coin_traits.MULTISIG){
                                return database.collection("coins_multisig").countDocuments({tx_id: {$eq: sp.spent_tx_id}, pos: {$eq: sp.pos}}).then(count => {
                                    if(count == 0){
                                        //spent missing, must have been consumed by another transaction on blockchain!
                                        rejects.add(sp.tx_id);
                                    }
                                });
                            }else{
                                //spent missing, must have been consumed by another transaction on blockchain!
                                rejects.add(sp.tx_id);
                            }
                        }
                    })
                }
            }));

            if(rejects.size > 0){
                let txids = Array.from(rejects);
                return Promise.all([
                    database.collection("rejects").insertMany(txids.map(x => {tx_id: x})),
                    this.removePendingTransactions(txids)
                ])
            }
        }
    }
}