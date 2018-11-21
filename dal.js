'use strict'

/**
 * Data Access Layer
 */

const DB_VERSION_V1 = 1; //int32-indexed "coins" & "payloads" collections
const DB_VERSION_V2 = 2; //int64-indexed "coins" & "payloads" collections
/**
 * V3: reorganize collections as following:
 * 
 * (1) coins/coins_multisig/coins_noaddress: uxto on blockchain 
 * (2) pending_coins/pending_coins_multisig/pending_coins: uxto in mempool backup_spent_coins: uxtos spent by recent might-be-rolled-back blocks 
 * (3) backup_blocks: recent might-be-rolled-back blocks 
 * (4) log: event logs 
 * (5) summary: generic info
 */
const DB_VERSION_V3 = 3; 
const LATEST_DB_VERSION = DB_VERSION_V3;

const BigNumber = require('bignumber.js');
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

async function getNextNoAddrCoinIdLong(){
    let r = await database.collection("coins_noaddr").find().sort({_id: -1}).limit(1).next();
    return r == null ? LONG_ONE : Long.fromNumber(r._id).add(LONG_ONE);
}

async function getNextPayloadIdLong(){
    let r = await database.collection("payloads").find().sort({_id: -1}).limit(1).next();
    return r == null ? LONG_ONE : Long.fromNumber(r._id).add(LONG_ONE);
}


module.exports = {
    LOG_LEVEL_TRACE: 0,
    LOG_LEVEL_INFO: 1,
    LOG_LEVEL_WARN: 2,
    LOG_LEVEL_ERROR: 3,
    LOG_LEVEL_FATAL: 4,

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
            let lastBlockInfo = await this.getLastRecordedBlockInfo();
            if(lastBlockInfo != null ){
                let ver = await this.getDBVersion();
                if(ver != LATEST_DB_VERSION){
                    //database version mismatch
                    dbg_throw_error(`Database version mismatch, the version expected: [${LATEST_DB_VERSION}] db_version: [${ver}], need to run upgrade once!`);
                }
            }

            await Promise.all([
                database.createCollection("coins"),
                support_multisig ? database.createCollection("coins_multisig") : Promise.resolve(),
                database.createCollection('coins_noaddr'),


                database.createCollection("pending_coins"),
                support_multisig ? database.createCollection("pending_coins_multisig") : Promise.resolve(),
                database.createCollection("pending_coins_noaddr"),

                database.createCollection("pending_spents"),
                support_multisig? database.createCollection("pending_spents_multisig") : Promise.resolve(),
                database.createCollection("pending_spents_noaddr"),

                support_payload ? database.createCollection("payloads") : Promise.resolve(),
                support_payload ? database.createCollection("pending_payloads") : Promise.resolve(),
                
                database.createCollection("rejects"),
                database.createCollection('backup_blocks'),
                database.createCollection('backup_spent_coins'),
                database.createCollection('logs'),
                database.createCollection('summary'),
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

                database.collection('coins_noaddr').createIndexes([
                    { key: {tx_id: 1, pos: 1}, name: "idx_xo", unique: true }, //multisig cannot appear in coinbase, so it should be unique
                ]),

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
                database.collection('pending_coins_noaddr').createIndexes([
                    { key: {tx_id: 1}, name: "idx_tx"}, 
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
                ]),

                database.collection('logs').createIndexes([
                    { key: { level: 1 }, name: "idx_level", unique: true }
                ]),

                database.collection('backup_blocks').createIndexes([
                    { key: {height: 1}, name: "idx_height", unique: true }, 
                    { key: {hash: 1}, name: "idx_hash", unique: true },
                ]),
                database.collection('backup_spent_coins').createIndexes([
                    { key: {height: 1}, name: "idx_height", unique: true }, 
                ]),
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

    async addLogs(logs){
        if(logs.length > 0){
            await database.collection("logs").insertMany(logs);
        }
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
        if(coins.length > 0){
            let N = await getNextCoinIdLong();
            coins.forEach(x=> { x._id = N; N = N.add(LONG_ONE); });
            await database.collection("coins").insertMany(coins);
        }
    },

    async addCoinsMultiSig(coins){
        if(coins.length > 0){
            let N = await getNextMultiSigCoinId();
            coins.forEach(x => x._id = N++);

            await database.collection("coins_multisig").insertMany(coins);
        }
    },

    async addCoinsNoAddr(coins){
        if(coins.length > 0){
            let N = await getNextNoAddrCoinIdLong();
            coins.forEach(x=> { x._id = N; N = N.add(LONG_ONE); });
            await database.collection("coins_noaddr").insertMany(coins);
        }
    },

    async addPayloads(payloads){
        //let N = await database.collection("payloads").countDocuments({}) + 1;
        let N = await getNextPayloadIdLong();
        payloads.forEach(x => { x._id = N; N = N.add(LONG_ONE); });
        return database.collection("payloads").insertMany(payloads);
    },

    async addSpents(spents){
        if(spents.length > 0){
            let ops = spents.map(spent => {
                return {
                    deleteOne: { "filter": {"tx_id": spent.spent_tx_id, "pos": spent.pos}}
                }
            });
            let no_order = { ordered: false };

            await Promise.all([
                database.collection("coins").bulkWrite( ops, no_order),
                database.collection("coins_noaddr").bulkWrite( ops,no_order), 
                config.coin_traits.MULTISIG ? database.collection("coins_multisig").bulkWrite( ops, no_order) : Promise.resolve(),
            ]);
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
        if(spents.length > 0){
            await database.collection("pending_spents").insertMany(spents);
        }
    },
    async addPendingSpentsMultiSig(spents){
        if(spents.length > 0){
            await database.collection("pending_spents_multisig").insertMany(spents);
        }
    },
    async addPendingSpentsNoAddr(spents){
        if(spents.length > 0){
            await database.collection("pending_spents_noaddr").insertMany(spents);
        }
    },
    async addPendingCoins(coins){
        if(coins.length > 0){
            await database.collection("pending_coins").insertMany(coins);
        }
    },
    async addPendingCoinsMultiSig(coins){
        if(coins.length > 0){
            await database.collection("pending_coins_multisig").insertMany(coins);
        }
    },
    async addPendingCoinsNoAddr(coins){
        if(coins.length > 0){
            await database.collection("pending_coins_noaddr").insertMany(coins);
        }
    },

    async addPendingPayloads(payloads){
        if(payloads.length > 0){
            await database.collection("pending_payloads").insertMany(payloads);
        }
    },

    /**
     * Delete all pending info related to incoming txids
     * 
     * @param {txids} txids new parsed transaction-ids on blockchain
     */
    async removePendingTransactions(txids){
        let ops = txids.map(txid => {
            return {
                deleteMany: { "filter": { "tx_id":  txid }}
            }
        });

        return Promise.all([
            database.collection("pending_coins").bulkWrite(ops, { ordered: false} ), 
            config.coin_traits.MULTISIG ? database.collection("pending_coins_multisig").bulkWrite(ops, { ordered: false} ) : Promise.resolve(),
            database.collection("pending_coins_noaddr").bulkWrite(ops, { ordered: false} ), 

            database.collection("pending_payloads").bulkWrite( ops, { ordered: false} ),

            database.collection("pending_spents").bulkWrite(ops, { ordered: false} ),
            config.coin_traits.MULTISIG ? database.collection("pending_spents_multisig").bulkWrite(ops, { ordered: false} ) : Promise.resolve(),
            database.collection("pending_spents_noaddr").bulkWrite(ops, { ordered: false} ),
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
                let items = txids.map(x => {
                    return {tx_id: x}
                });
                await Promise.all([
                    database.collection("rejects").insertMany(items),
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

                dbg.info("V1=>V2 Upgrade Successfully!");
            }
        }
    },
    //-------------- V2 => V3 ---------------
    async upgradeV2toV3(dbg){
        //errors => coins_noaddr
        await database.createCollection('coins_noaddr');
        await database.collection('coins_noaddr').createIndexes([
            { key: {tx_id: 1, pos: 1}, name: "idx_xo", unique: true }, //multisig cannot appear in coinbase, so it should be unique
        ]);

        const last_upgrade_item = 'last_upgrade_item';

        dbg.info('Counting total items to upgrade...');
        let tbErrors = database.collection('errors');
        let N = await tbErrors.countDocuments({});
        dbg.info(`Total ${N} items to upgrade!`);

        if(N > 0){
            const coin_traits = config.coin_traits;
            let tbCoins = database.collection('coins_noaddr');

            let i = await getLastValue(last_upgrade_item);
            if(i != null){
                i++; //start of next batch
            }else{
                i = 0;
            } 
            if(i < N){
                dbg.info(`delete all *dirty* items in target table from item[${i}]...`);
                await tbCoins.deleteMany({_id: {$gte: Long.fromInt(i)}});
            }

            let j = i + config.batch_blocks;
            if(j > N) j = N;

            while(i < N) {
                if(stopping) break;

                dbg.info(`upgrading [${i}, ${j})...`);

                let items = await tbErrors.find().sort({_id: 1}).skip(i).limit(j-i).toArray();

                await tbCoins.insertMany(items.filter(x => x.height >=0).map((x, k) => {
                    let out = x.tx_info.vout[x.pos];
                    let vCoin = new BigNumber(out.value); 
                    return {
                        _id: Long.fromInt(i + k),
                        tx_id: x.tx_id,
                        pos: x.pos,
                        value: vCoin.multipliedBy(coin_traits.SAT_PER_COIN).toString(),
                        height: x.height,
                        script: out.scriptPubKey
                    } 
                }));

                await setLastValue(last_upgrade_item,j-1);

                i = j;
                if( i < N){
                    j += config.batch_blocks;
                    if(j > N) j = N;
                }
            }

            let M = await getLastValue(last_upgrade_item);
            if( M == N-1){
                //dbg.info("Upgrade Successfully! (FAKE)");
                //return; 
                //complete upgrade
                await tbErrors.drop();
                await deleteLastValue(last_upgrade_item);
                await this.setDBVersion(LATEST_DB_VERSION);

                //Remove useless field
                let bi = await this.getLastRecordedBlockInfo();
                if(bi != null){
                    await database.collection('summary').deleteOne({_id: 'lastBlockHeight'});
                }

                dbg.info("V2=>V3 Upgrade Successfully!");
            }
        }
    }
}