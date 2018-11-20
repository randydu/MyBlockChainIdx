'use strict'

/**
 * Data Access Layer
 */

const common = require('./common');
const debug = common.create_debug('dal');
const config = common.config;
const dbg_throw_error = common.dbg_throw_error(debug);

const Long = require('mongodb').Long;
const LONG_ONE = Long.fromInt(1);

const MongoClient = require('mongodb').MongoClient;
var client = null;
var database = null; 

var stopping = false; //user stop


async function setLastValue(key, v){
    await database.collection("summary").findOneAndUpdate(
        { _id: key }, 
        {$set: {value: v}},
        { upsert: true }
    );
}

async function getLastValue(key){
    let r = await database.collection("summary").findOne({ _id: key });
    return r == null ? null : r.value;
}

async function deleteLastValue(key){
    await database.collection('summary').deleteOne({_id: key});
}

/* deprecated. (DB_VERSION_V1)
async function getNextCoinIdInt32(){
    let r = await database.collection("coins").find().sort({_id: -1}).limit(1).next();
    return r == null ? 1 : r._id + 1;
}
*/
async function getNextCoinIdLong(){
    let r = await database.collection("coins").find().sort({_id: -1}).limit(1).next();
    return r == null ? LONG_ONE : Long.fromNumber(r._id).add(LONG_ONE);
}

async function getNextMultiSigCoinId(){
    let r = await database.collection("coins_multisig").find().sort({_id: -1}).limit(1).next();
    return r == null ? 1 : r._id + 1;
}

async function getNextPayloadIdLong(){
    let r = await database.collection("payloads").find().sort({_id: -1}).limit(1).next();
    return r == null ? LONG_ONE : Long.fromNumber(r._id).add(LONG_ONE);
}

const DB_VERSION_V1 = 1; //int32-indexed "coins" & "payloads" collections
const DB_VERSION_V2 = 2; //6432-indexed "coins" & "payloads" collections
const LATEST_DB_VERSION = DB_VERSION_V2;


module.exports = {

    async init(do_upgrade = false){
        debug.info("dal.init >>");

        let mongodb_url = process.env.MONGODB_URL;
        debug.info("MONGODB_URL=%s", mongodb_url);
        
        client = await MongoClient.connect(mongodb_url, { useNewUrlParser: true });
        if(!client.isConnected()){
            dbg_throw_error("database not connected!");
        }

        database = client.db('myidx');

        const support_payload = config.coin_traits.payload;
        const support_multisig = config.coin_traits.MULTISIG;

        if(!do_upgrade){
            let lastBlockHeight = await this.getLastRecordedBlockHeight();
            if(lastBlockHeight >= 0){
                let ver = await this.getDBVersion();
                if(ver != LATEST_DB_VERSION){
                    //database version mismatch
                    dbg_throw_error(`Database version mismatch, the version expected: [${LATEST_DB_VERSION}] db_version: [${ver}], need to run upgrade once!`);
                }
            }

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

            await this.setDBVersion(LATEST_DB_VERSION);
        }

        debug.info("dal.init <<");
    },

    stop(){
        stopping = true;
    },

    async close(){
        if(client) await client.close();
    },

    //The current db version supported.
    getLatestDBVersion(){
        return LATEST_DB_VERSION;
    },

    async getDBVersion(){
        let v = await getLastValue("db_version");
        return v == null ? 1 : v;
    },

    async setDBVersion(db_ver){
        await setLastValue("db_version", db_ver);
    },

    async setCoinInfo(ci){
        let pre_ci = await getLastValue("coin");
        if(pre_ci == null){
            await setLastValue("coin", ci);
        }else{
            if(ci.coin != pre_ci.coin || ci.network != pre_ci.network){
                dbg_throw_error("coin info mismatch!");
            }
        }
    },

    async getLastRecordedBlockInfo(){
        return await getLastValue('last_recorded_block');
    },
    async setLastRecordedBlockInfo(bi){
        await setLastValue('last_recorded_block', bi);
    },
    async getLastSafeBlockInfo(){
        return await getLastValue('last_safe_block');
    },
    async setLastSafeBlockInfo(bi){
        await setLastValue('last_safe_block', bi);
    },
    
    async getLastRecordedBlockHeight(){
        return await getLastValue('lastBlockHeight');
    },
    async setLastRecordedBlockHeight(height){
        await setLastValue('lastBlockHeight', height);
    },

    async addErrors(errs){
        return database.collection("errors").insertMany(errs);
    },

    async removeCoinsAfterHeight(height){
        return database.collection("coins").deleteMany({ height: { $gt: height } });
    },
    async removeMultiSigCoinsAfterHeight(height){
        return database.collection("coins_multisig").deleteMany({ height: { $gt: height } });
    },
    async removePayloadsAfterHeight(height){
        return database.collection("payloads").deleteMany({ height: { $gt: height } });
    },

    async addCoins(coins){
        let N = await getNextCoinIdLong();
        coins.forEach(x=> { x._id = N; N = N.add(LONG_ONE); });
        return database.collection("coins").insertMany(coins);
    },

    async addMultiSigCoins(coins){
        let N = await getNextMultiSigCoinId();
        coins.forEach(x => x._id = N++);

        return database.collection("coins_multisig").insertMany(coins);
    },

    async addPayloads(payloads){
        //let N = await database.collection("payloads").countDocuments({}) + 1;
        let N = await getNextPayloadIdLong();
        payloads.forEach(x => { x._id = N; N = N.add(LONG_ONE); });
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
                deleteMany: { "filter": { "tx_id":  txid }}
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
                await Promise.all([
                    database.collection("rejects").insertMany(txids.map(x => {tx_id: x})),
                    this.removePendingTransactions(txids)
                ])

                rejects.clear();
            }
            rejects = null;
        }
    },
    //-------------- V1 => V2 ---------------
    async upgradeV1toV2(dbg){
        //check if coin_v1 already exists, in case we may pick up from previous incomplete upgrading.
        let has_coins_v1 = (await database.collections()).some(x => x.collectionName == 'coins_v1');

        if(!has_coins_v1){
            dbg.info('fresh new upgrade...');
            await database.collection('coins').rename('coins_v1');
        }

        await database.createCollection("coins");
        await database.collection('coins').createIndexes([
            { key: {address: 1}, name: "idx_addr" }, 
            { key: {height: 1}, name: "idx_height" }, 
            { key: {tx_id: 1, pos: 1}, name: "idx_xo", unique: config.coin_traits.BIP34 },//multiple (tx_id,pos) in coinbase pre-BIP34
        ]);

        const last_upgrade_item = 'last_upgrade_item';

        dbg.info('Counting total items to upgrade...');
        let tbCoinsV1 = database.collection('coins_v1');
        let N = await tbCoinsV1.countDocuments({});
        dbg.info(`Total ${N} items to upgrade!`);

        if(N > 0){
            let tbCoins = database.collection('coins');

            let i = await getLastValue(last_upgrade_item);
            if(i != null){
                i++; //start of next batch
            }else{
                i = 0;
            } 
            if(i < N){
                dbg.info(`delete all *dirty* items in target table from item[${i}]...`);
                let item = await tbCoinsV1.find().sort({_id:1}).skip(i).next();
                await tbCoins.deleteMany({_id: {$gte: Long.fromInt(item._id)}});
            }

            let j = i + config.batch_upgradeV1toV2;
            if(j > N) j = N;

            while(i < N) {
                if(stopping) break;

                dbg.info(`upgrading [${i}, ${j})...`);

                let items = await tbCoinsV1.find().sort({_id: 1}).skip(i).limit(j-i).toArray();
                items.forEach(x => { x._id = Long.fromInt(x._id) });
                await tbCoins.insertMany(items);

                await setLastValue(last_upgrade_item,j-1);

                i = j;
                if( i < N){
                    j += config.batch_upgradeV1toV2;
                    if(j > N) j = N;
                }
            }

            let M = await getLastValue(last_upgrade_item);
            if( M == N-1){
                //dbg.info("Upgrade Successfully! (FAKE)");
                //return; 
                //complete upgrade
                await tbCoinsV1.drop();
                await deleteLastValue(last_upgrade_item);
                await this.setDBVersion(LATEST_DB_VERSION);

                dbg.info("Upgrade Successfully!");
            }
        }
    }
}