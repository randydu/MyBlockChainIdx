'use strict'

const common = require('./common');

const BigNumber = require('bignumber.js');

const debug = common.create_debug('sam');

const config = common.config;
const node = config.node;
const coin_traits = config.coin_traits;
const support_payload = coin_traits.payload;
const use_rest_api = config.use_rest_api;

const dal = require('./dal');

const Client = require('bitcoin-core');

//initialized in init();
var client = null; 

let latest_block = -1; //the latest block height reported by coin's RPC-API.
var pending_txids = new Set(); //pending txids

//When it is the first time checking mempool, the pending txs might have been saved to database before
//after that the pending_txids will avoid saving the same pending tx to database.
var first_time_save_pendings = true;

var first_time_check_blocks = true;
////////////////////////////////////////////////////////////////

const api = require('./api');

function postStatus(status){
    api.setStatus(status);
}

function cloneObj(obj){
    let res = {};
    for(let prop in obj){
        if(prop == "_id") continue;
        if(obj.hasOwnProperty(prop)){
            res[prop] = obj[prop];
        }
    }
    return res;
}

async function getLatestBlockCount(){
    if(use_rest_api){
        let r = await client.getBlockchainInformation();
        return r.blocks;
    }else{
        return client.getBlockCount();
    }
}

async function getTransactionInfo(txid){
    if(use_rest_api){
        return client.getTransactionByHash(txid);
    }else{
        return client.getRawTransaction(txid, coin_traits.getrawtransaction_verbose_bool ? true : 1);
    }
}

async function process_tis(blk_tis){
    //spents of txs on blockchain
    let spents_remove = [];
    let spents_backup = []; 

    //spents of txs in mempool
    let pending_spents = [];
    let pending_spents_multisig = [];
    let pending_spents_noaddr = [];

    //coins on either blockchain or mempool
    let coins = [];
    let coins_multisig = [];
    let coins_noaddr = [];
    
    let payloads = [];
    let txids = [];

    let logs = [];

    let pending = false;

    for(let k = 0; k < blk_tis.length; k++){
        let { height, tis } = blk_tis[k];
        
        if(height < 0) pending = true;
        
        const N = tis.length;

        for(let i = 0; i < N; i++){

            let tx_info = tis[i];
            let txid = tx_info.txid;

            debug.trace('height[%d] >> [%d/%d] txid: %s', height, i, N, txid);

            txids.push(txid);
            /**
             * 
             * RPC:
             * {
                "hex" : "02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1e029b00062f503253482f0409d47f5b0881000003000000007969696d700000000000010000e1f5050000001976a914a2757813e8c98fa81b0e14574aeb8a1891ed0f5f88ac000000000009d47f5b",
                "txid" : "4899bb0e9b692e182a10705c0726d540edaa95567f0190ad399628e713a77ae3",
                "version" : 2,
                "txtime" : 1535104009,
                "locktime" : 0,
                "vin" : [
                    {
                        "coinbase" : "029b00062f503253482f0409d47f5b0881000003000000007969696d7000",
                        "sequence" : 0
                    }
                ],
                "vout" : [
                    {
                        "value" : 256.00000000,
                        "n" : 0,
                        "scriptPubKey" : {
                            "asm" : "OP_DUP OP_HASH160 a2757813e8c98fa81b0e14574aeb8a1891ed0f5f OP_EQUALVERIFY OP_CHECKSIG",
                            "hex" : "76a914a2757813e8c98fa81b0e14574aeb8a1891ed0f5f88ac",
                            "reqSigs" : 1,
                            "type" : "pubkeyhash",
                            "addresses" : [
                                "BKG5oggWaVeKYTRM4aVAzKc48ATCXXSyZb"
                            ]
                        },
                        "payloadHint" : 0,
                        "payloadSubHint" : 0,
                        "payloadSize" : 0,
                        "payload" : ""
                    }
                ],
                "blockhash" : "000000031757ff0d32fceaf68fde9444e90391542120d6776ac3f0e30a9401ee",
                "confirmations" : 28218,
                "time" : 1535104009,
                "blocktime" : 1535104009
            }

            */



            let ins = tx_info.vin;
            if(ins.length > 0){
                for(let j = 0; j < ins.length; j++){
                    let spent = ins[j];
                    if(spent.txid){//coinbase won't have txid defined
                        let spent_cancelled = false;
                        if(config.coins_spent_in_mem_cancel){
                            //Detect if spent the same utxos (previous txs) in the same block.
                            //Cancel the (coin, spent) pair in memory
                            function is_spent_cancelled(coins_arr, sp_txid, sp_pos){
                                for(let m = 0; m < coins_arr.length; m++){
                                    let coin = coins_arr[m];
                                    if((coin.tx_id == sp_txid) && (coin.pos == sp_pos)){
                                        /**
                                         * First matched coin is spent.
                                         * For non-BIP34 compatible cryptocurrency, there might be more than one matched coins, we want only one coin 
                                         * (the first matched) is cancelled, that is why we cannot use the following code:
                                         * 
                                         *  coins = coins.filter(coin => (coin.tx_id != spent.txid)||(coin.pos != spent.vout));
                                         * 
                                         *  it may spend multiple coins with a single spent.
                                         */
                                        coins_arr.splice(m, 1);
                                        return true;
                                    }
                                }
                                return false;
                            }

                            spent_cancelled = is_spent_cancelled(coins, spent.tx_id, spent.vout)
                                    || is_spent_cancelled(coins_multisig, spent.tx_id, spent.vout)
                                    || is_spent_cancelled(coins_noaddr, spent.tx_id, spent.vout);
                        }

                        if(!spent_cancelled){
                            let obj = {
                                tx_id: txid,
                                spent_tx_id: spent.txid,
                                pos: spent.vout
                            }
                            
                            if(pending){
                                //(address, value) is only needed for pending spents, the coins table only holds unspent xo.
                                //TODO: we can retrieve the (address, value) info from dal --- the coin should already be in database.
                                let tx_spent = await getTransactionInfo(spent.txid);
                                if(tx_spent == null){
                                    obj.level = dal.LOG_LEVEL_WARN; //just affect the balance::pending value
                                    obj.message = `spent tx [${spent.txid}] not found`;
                                    obj.code = 'TX_NOT_FOUND';
                                    logs.push(obj);

                                    dbg.warn(obj.message);
                                } else {
                                    if(tx_spent.vout.length <= spent.vout){
                                        obj.code = 'SPENT_NOT_FOUND';
                                        obj.level = dal.LOG_LEVEL_WARN; //just affect the balance::pending value
                                        obj.message = `Spent UTXO not found, tx_spent.vout.length = [${tx_spent.vout.length}] <= spent.vout [${spent.vout}]`;
                                        logs.push(obj);

                                        dbg.warn(obj.message);
                                    } else {
                                        let out = tx_spent.vout[spent.vout];
                                        let vCoin = new BigNumber(out.value); 
                                        obj.value = vCoin.multipliedBy(coin_traits.SAT_PER_COIN).toString();

                                        if(typeof out.scriptPubKey.addresses === 'undefined'){
                                            obj.script = out.scriptPubKey;
                                            pending_spents_noaddr.push(obj);
                                        }else{
                                            if(out.scriptPubKey.addresses.length != 1){
                                                obj.addresses = out.scriptPubKey.addresses;
                                                pending_spents_multisig.push(obj);
                                            }else{
                                                obj.address = out.scriptPubKey.addresses[0];
                                                pending_spents.push(obj);
                                            }
                                        }
                                    }
                                }
                            }else{
                                if(latest_block - height < coin_traits.max_confirms){ 
                                    obj.height = height; //backed up spent need height to rollback later if needed.
                                    spents_backup.push(obj);
                                } else {
                                    spents_remove.push(obj);
                                }
                            }
                        }
                    }
                }                    
            }

            let outs = tx_info.vout;
            if(outs.length > 0){
                for(let j = 0; j < outs.length; j++){
                    let out = outs[j];
                    if(out.value >= 0){//only save non-zero utxo
                        let vCoin = new BigNumber(out.value);
                        vCoin = vCoin.multipliedBy(coin_traits.SAT_PER_COIN);

                        let obj = {
                            tx_id: txid,
                            pos: j, //the j'th output in the tx
                            value: vCoin.toString(),
                        };
                        if(!pending) obj.height = height; //pending_xxx does not have height field.

                        if(typeof out.scriptPubKey.addresses === 'undefined'){//no-address
                            if(out.value > 0){
                                obj.script = out.scriptPubKey;
                                coins_noaddr.push(obj);
                            } 
                        }else if(out.scriptPubKey.addresses.length > 1){ //multisig /multi-address
                            //MULTISIG support
                            if(out.value > 0){
                                obj.addresses = out.scriptPubKey.addresses;
                                coins_multisig.push(obj);
                            }
                        }else{ //single-address
                            obj.address = out.scriptPubKey.addresses[0];
                            if(out.value > 0) coins.push(obj);

                            //payload can only be parked on single-address vout
                            if(support_payload && (out.payloadSize > 0)){
                                obj.hint = out.payloadHint;
                                obj.subhint = out.payloadSubHint;
                                obj.size = out.payloadSize;
                                obj.payload = out.payload;
                                
                                payloads.push(obj);
                            }
                        }
                    }
                }
            }
        }
    }

    if(!pending){
        if(txids.length > 0){
            //remove local cached txids
            txids.forEach(txid => {
                pending_txids.delete(txid);
            });

            //now that tx is mined on blockchain, it should be removed from pending records.
            await dal.removePendingTransactions(txids);
        }

        if(support_payload && (payloads.length > 0)){
            await dal.addPayloads(payloads);
        }

        //addCoins before addSpent! (to support spent uxio of tx in the same block)
        await Promise.all([
            dal.addCoins(coins),
            dal.addCoinsMultiSig(coins_multisig),
            dal.addCoinsNoAddr(coins_noaddr)
        ]);

        await dal.removeSpents(spents_remove);
        await dal.backupSpents(spents_backup);

    } else { //pending
        if(first_time_save_pendings){
            //avoid key-collision in case the pending tx is already saved in database in last session
            await dal.removePendingTransactions(txids);
            first_time_save_pendings = false;
        }
        await Promise.all([
            dal.addPendingSpents(pending_spents),
            dal.addPendingSpentsMultiSig(pending_spents_multisig),
            dal.addPendingSpentsNoAddr(pending_spents_noaddr),

            dal.addPendingCoins(coins),
            dal.addPendingCoinsMultiSig(coins_multisig),
            dal.addPendingCoinsNoAddr(coins_noaddr),

            dal.addPendingPayloads(payloads),
        ]);
    }

    //logs
    await dal.logEvent(logs);

    //manually release memory
    /*
    spents.length = 0;
    coins.length = 0;
    coins_multisig.length = 0;
    payloads.length = 0;
    errs.length = 0;
    txids.length = 0;
    */
}

async function sample_pendings(){
    debug.info('check pendings >>');

    let txids = await client.getrawmempool();
    if(txids.length > 0){
        let new_txids = []; //new pending txs to save to database.

        if(pending_txids.size > 0){
            txids.forEach(txid => {
                if(!pending_txids.has(txid)){
                    new_txids.push(txid);
                }
            });
        }else new_txids = txids;

        let tis = [];
        if(new_txids.length > 0){
            for(let i = 0; i < new_txids.length; i++){
                let txid = new_txids[i];
                let ti = await getTransactionInfo(txid);
                if(ti == null){
                    dbg.throw_error(`transaction [${txid}]  not found!`);
                }
                tis.push(ti);
            };

            await process_tis([{
                height: -1, //pending
                tis: tis
            }]);

            //now update on success
            new_txids.forEach(txid => {
                pending_txids.add(txid);
            });
        }
    }

    debug.info('check pendings <<');
}

async function check_rejection(){
    debug.info('check tx rejection >>');

    await dal.check_rejection();

    debug.info('check tx rejection <<');
}

let start_blk_hash = null; //rest-only
let end_blk_hash = null; //last recorded block hash

/**
 * nStart: block_hash(nStart) == start_blk_hash; (REST)
 * nEnd: next start of batch
 */

async function sample_batch(nStart, nEnd /* exclude */) {
    let blk_tis = [];
    let blks = [];

    debug.info(`sync [${nStart}, ${nEnd})...`);

    if(use_rest_api){
        //rest-api
        if(start_blk_hash == null){
            start_blk_hash = await client.getBlockHash(nStart);
        }
        let hdrs = await client.getBlockHeadersByHash(start_blk_hash, nEnd - nStart);
        for(let i = nStart; i < nEnd; i++){
            if(latest_block - i < coin_traits.max_confirms){
                blks.push({
                    _id: i,
                    hash: hdrs[i-nStart].hash
                });
            }

            let blk_info = await client.getBlockByHash(hdrs[i-nStart].hash);

            if(blk_info.height != i){
                dbg.throw_error(`block height mismatch, expected: ${i} got: ${blk_info.height}`);
            }

            blk_tis.push({
                height: i,
                tis: blk_info.tx //tx_info already populated by REST-api
            });
        }
        start_blk_hash = hdrs[nEnd-nStart-1].nextblockhash;
        end_blk_hash = hdrs[nEnd-nStart-1].hash;
    }else{
        //rpc-api
        for(let i = nStart; i < nEnd; i++){
            let item = { height: i, tis: [] };

            let blk_hash = await client.getBlockHash(i);
            if(i == nEnd-1){
                end_blk_hash = blk_hash;
            }

            if(latest_block - i < coin_traits.max_confirms){
                blks.push({
                    _id: i,
                    hash: blk_hash
                });
            }

            let blk_info = await client.getBlock(blk_hash, coin_traits.getblock_verbose_bool ? true : 1);
            /**
             * {
            "hash" : "000000078ffb6946a3f964d39f70f1fe5d1215c40684526a590c1d6ef1682860",
            "confirmations" : 28153,
            "size" : 201,
            "height" : 202,
            "version" : 2,
            "merkleroot" : "0b6c93af98836524d909101d31a84e0b63a78af8e755d343c084c6302d26c54c",
            "time" : 1535106745,
            "nonce" : 2494895360,
            "bits" : "1d0a17ff",
            "difficulty" : 0.09906985,
            "mint" : 256.00000000,
            "previousblockhash" : "000000028504c467f5d8bb23cb2e28af7b4b4d2e11261109edae624857d3ffa3",
            "nextblockhash" : "00000000585cd57b80d92072d5b623616ad49e1b78db1245688d1dc1eb9eeef8",
            "flags" : "proof-of-work",
            "proofhash" : "000000078ffb6946a3f964d39f70f1fe5d1215c40684526a590c1d6ef1682860",
            "entropybit" : 0,
            "modifier" : "0000000000000000",
            "modifierchecksum" : "e7903dbd",
            "tx" : [
                "0b6c93af98836524d909101d31a84e0b63a78af8e755d343c084c6302d26c54c"
                ]
            }
            */
            let txs = blk_info.tx;

            for(let j = 0; j < txs.length; j++){
                let ti = await client.getRawTransaction(txs[j], coin_traits.getrawtransaction_verbose_bool ? true : 1);
                item.tis.push(ti);
            }

            blk_tis.push(item);
        }    
    }

    await process_tis(blk_tis);
    if(blks.length > 0) await dal.addBackupBlocks(blks);

    //manually release resources
    //blk_tis.forEach(x => { x.tis.length = 0; });
    //blk_tis.length = 0;

    debug.info(`sync [${nStart}, ${nEnd}) done!`);
}
////////////////////////////////////////////////////////////////
module.exports = {
    stop: false, //flag to indicate shut down immediately

    async init(){
        debug.trace('sam.init >> ');

        postStatus('INIT');

        //full-node accessor
        client = new Client({
            version: node.rpcversion,
            network: node.network,
            ssl: {
                enabled: false,
            },
            host: node.rpchost,
            username: node.rpcuser,
            password: node.rpcpassword,
            port: node.rpcport,
            timeout: node.timeout
        });

        common.add_apis(client, coin_traits.apis);

        //test if node is accessible
        debug.trace('waiting for node...');

        let count = process.env.NODE_WAIT_TIMEOUT_SECONDS || 10; //10
        let ready = false;
        while(!ready && (count-- > 0)){
            debug.trace(`waiting...${count}`);

            try {   
                let info = await client.getInfo();
                debug.trace("%O", info);
                
                ready = true;
            }catch(err){
                debug.err(`err: ${err.message}`);
            }

            if(!ready) await common.delay(1000);
        }

        if(!ready) throw new Error('full node not found, abort!');

        await dal.setCoinInfo({
            coin: node.coin,
            network: node.network
        })

        debug.trace('sam.init << ');
    },

    async run(){
        debug.info('sam.run >> ');

        latest_block = await getLatestBlockCount();
        debug.info(`latest block: ${latest_block}`);

        //detect if the last recorded block is a valid one
        let last_recorded_blocks = null;
        let last_recorded_bi = await dal.getLastRecordedBlockInfo();
        if(last_recorded_bi != null){
            last_recorded_blocks = last_recorded_bi.height;
        }else{
            last_recorded_blocks = await dal.getLastRecordedBlockHeight();
        }
        if(last_recorded_blocks == null) last_recorded_blocks = -1; 
        debug.info(`latest recorded block: ${last_recorded_blocks}`);

        let obj = {
            last_recorded_blocks,
            latest_block,
        };

        let to_rollback = false; //need rollback due to blockchain reorganization
        if(latest_block < last_recorded_blocks){
            to_rollback = true;
        }else{
            if(last_recorded_bi != null){
                let blk_hash = await client.getBlockHash(last_recorded_blocks);
                if((last_recorded_bi.hash != null) && (blk_hash != last_recorded_bi.hash)){

                    obj.last_recorded_hash = last_recorded_bi.hash;
                    obj.latest_block_hash = blk_hash;

                    to_rollback = true;
                }
            }
        }

        let last_good_block = -1;
        if(to_rollback){
            //dbg_throw_error("ROLLBACK detected, PLEASE DEBUG ME!");

            obj.message = 'start';
            await dal.logEvent( obj, 'ROLLBACK', dal.LOG_LEVEL_WARN);

            debug.warn('rollback blockchain...');

            let last_good_block_hash = '';
            let blks = await dal.getBackupBlocks();
            for(let i = blks.length-1; i >= 0; i--){
                let blk = blks[i];
                let blk_hash = await client.getBlockHash(blk._id);
                if(blk_hash == blk.hash){
                    last_good_block = blk._id;
                    last_good_block_hash = blk_hash;
                    break;
                }
            }

            if(last_good_block == -1){
                obj.message = 'failure: not enough backup blocks!';
                await dal.logEvent(cloneObj(obj), 'ROLLBACK', dal.LOG_LEVEL_FATAL);

                dbg.throw_error('rollback failure: not enough backup blocks!'); 
            }

            obj.last_good_block = last_good_block;
            obj.last_good_block_hash = last_good_block_hash;

            await dal.rollback(last_good_block);

            last_recorded_blocks = last_good_block;
            first_time_check_blocks = true;

            first_time_save_pendings = true;
            pending_txids.clear();

            obj.message = 'done';
            await dal.logEvent( cloneObj(obj), 'ROLLBACK', dal.LOG_LEVEL_WARN);

            debug.warn(`blockchain is rolled back to ${last_good_block} successfully!`);
        }

        postStatus(latest_block - last_recorded_blocks > process.env.SYNC_TOLERANCE ? 'SYNCING' : 'OK');

        if(latest_block > last_recorded_blocks){

            if(first_time_check_blocks){
                first_time_check_blocks = false;

                //in case the previous session is not completed.
                //avoid rollback twice if just rollback before.
                if(last_good_block != -1) await dal.rollback(last_recorded_blocks);
            }

            let i = last_recorded_blocks + 1; //start blk# of this batch
            if((i == 0) && !coin_traits.genesis_tx_connected) i++; //skip genesis block if its tx is not used.

            let j = i + config.batch_blocks; //start blk# of next batch
            if(j > latest_block) j = latest_block + 1; //last batch

            while(i <= latest_block){
                if(this.stop) break;

                await sample_batch(i, j);

                //await dal.setLastRecordedBlockHeight(j-1);
                await dal.setLastRecordedBlockInfo({
                    height: j-1,
                    hash: end_blk_hash
                });

                i = j;
                j = i + config.batch_blocks;                
                if(j > latest_block) j = latest_block + 1; //last batch
            }
        }

        postStatus('OK');

        if(!this.stop){
            //pendings
            await sample_pendings();
        }
        if(!this.stop){
            //rejection
            await check_rejection();
        }

        if(!this.stop){
            //retire backup blocks and spents
            await Promise.all([
                dal.retireBackupBlocks(latest_block - config.coin_traits.max_confirms),
                dal.retireBackupSpents(latest_block - config.coin_traits.max_confirms),
            ]);
        }

        debug.info('sam.run << ');
    }
}
