'use strict'

const common = require('./common');

const BigNumber = require('bignumber.js');

const debug = require('mydbg')('sam');

const config = common.config;
const node = config.node;
const is_BPX = node.coin == 'bpx'; ///< we are sampling BlocPal blockchain


const coin_traits = config.coin_traits;
const support_payload = coin_traits.payload;

const use_rest_api = config.use_rest_api;
const resolve_spending = config.resolve_spending;

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
/**
 * Get decoded transaction information by tx-id
 * 
 * WARN: the conversion of vout.value from internal SAT to external coin unit (BTC/LTC/BCH, etc.) will
 * introduce precision error, BPX has added extra RPC param (value_unit) to avoid the conversion.
 * 
 * @param {string} txid - transaction id
 * @returns decoded transaction information
 *  
 *  BPX-specific:
 *      - height: bpx has a height field;
 *      - value unit: bpx returns vout.value as SAT instead of BPX with rpc param "value_unit" = 1
 */
async function getTransactionInfo(txid){
    if(use_rest_api){
        return client.getTransactionByHash(txid);
    }else{
        return is_BPX ? 
            /**
             * returns value in SAT to avoid conversion precision error
             */ 
            client.getRawTransaction(txid, coin_traits.getrawtransaction_verbose_bool ? true : 1, 0, 1 /* value_unit: 0: BPX, 1: SAT*/) : 
            client.getRawTransaction(txid, coin_traits.getrawtransaction_verbose_bool ? true : 1);
    }
}

/**
 * Convert the vout.value to amount in SAT
 * 
 * @param {vout} vout - transaction's vout object 
 * @returns {string} string of amount in SAT
 */
function getVoutAmount(vout){
    if(is_BPX){
        return vout.value.toString(); ///< BPX alreadys returns SAT! 
    }else{
        let vCoin = new BigNumber(vout.value); 
        return vCoin.multipliedBy(coin_traits.SAT_PER_COIN).toString();
    }
}

async function getBlockHeight(blk_hash){
    let blk_info = null;
    if(use_rest_api){
        blk_info = await client.getBlockByHash(blk_hash);
    }else{
        blk_info = await client.getBlock(blk_hash, coin_traits.getblock_verbose_bool ? true : 1);
    }
    if(blk_info != null){
        return +blk_info.height;
    }else{
        debug.warn(`getBlockHeight: blk_hash [${blk_hash}] not found!`);
        return -1;
    }
}

async function process_tis(blk_tis){
    //spents of txs on blockchain
    let spents_remove = [];
    let spents_backup = []; 

    //spents of txs in mempool
    let pending_spents_bare = []; //! resolve_spending

    let pending_spents = []; //resolve_spending
    let pending_spents_multisig = [];//resolve_spending
    let pending_spents_noaddr = [];//resolve_spending

    //coins on either blockchain or mempool
    let coins = [];
    let coins_multisig = [];
    let coins_noaddr = [];
    
    let payloads = [];
    let txids = [];

    let logs = [];

    let last_blk_done_height = -1; //all txs of block are processed
    let last_blk_done_hash = '';

    let pending = false;

    for(let k = 0; k < blk_tis.length; k++){
        let { height, blk_hash, tis, count, total_txs } = blk_tis[k];

        if(height < 0) pending = true;

        if(!pending && (count == total_txs)){
            last_blk_done_height = height;
            last_blk_done_hash = blk_hash;
        } 

        const N = tis.length;

        for(let i = 0; i < N; i++){

            let tx_info = tis[i];
            let txid = tx_info.txid;

            //debug.trace('height[%d] >> [%d/%d] txid: %s', height, i, N, txid);

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
                                //TODO: (Mar. 29, 2019) The processing of pending tx is too time-consuming if there are many pending txs 
                                //(such as BTC, there are even 1M), so we cannot resolve all information during the sampling phrase.
                                // the (address, value, height) are only needed by getBalance()::spending field.
                                //
                                //(address, value) is only needed for pending spents, the coins table only holds unspent xo.
                                //TODO: we can retrieve the (address, value) info from dal --- the coin should already be in database.
                                //However, the tx_spent might only exists in mempool.
                                if(resolve_spending){
                                    let tx_spent = await getTransactionInfo(spent.txid);
                                    if(tx_spent == null){
                                        obj.level = dal.LOG_LEVEL_WARN; //just affect the balance::pending value
                                        obj.message = `spent tx [${spent.txid}] not found`;
                                        obj.code = 'TX_NOT_FOUND';
                                        logs.push(obj);

                                        debug.warn(obj.message);
                                    } else {
                                        if(tx_spent.vout.length <= spent.vout){
                                            obj.code = 'SPENT_NOT_FOUND';
                                            obj.level = dal.LOG_LEVEL_WARN; //just affect the balance::pending value
                                            obj.message = `Spent UTXO not found, tx_spent.vout.length = [${tx_spent.vout.length}] <= spent.vout [${spent.vout}]`;
                                            logs.push(obj);

                                            debug.warn(obj.message);
                                        } else {
                                            let out = tx_spent.vout[spent.vout];
                                            obj.value = getVoutAmount(out);

                                            if(typeof tx_spent.blockhash === 'undefined'){
                                                obj.height = -1; //in mempool
                                            }else{
                                                /**
                                                 * we have tx_spent.confirmations, which can calculate the height = latest_blk - confirmations + 1
                                                 * but the latest_blk might not be the same value RPC-api used to calculate the confirmations, so we
                                                 * have to rely on api to figure out the right answer 
                                                 */
                                                if(tx_spent.height){
                                                    //BPX (maybe other coin?) has "height" field from getrawtransaction().
                                                    obj.height = +tx_spent.height;
                                                } else {
                                                    obj.height = await getBlockHeight(tx_spent.blockhash);
                                                }
                                            }

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
                                    //no need to resolve spending details
                                    pending_spents_bare.push(obj);
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
                        let obj = {
                            tx_id: txid,
                            pos: j, //the j'th output in the tx
                            value: getVoutAmount(out),
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

        //now we have fully processed a block!
        if(last_blk_done_height > 0){
            await dal.setLastRecordedBlockInfo({
                height: last_blk_done_height,
                hash: last_blk_done_hash
            })
        }

    } else { //pending
        if(first_time_save_pendings){
            //avoid key-collision in case the pending tx is already saved in database in last session
            //await dal.removePendingTransactions(txids);
            await dal.removeAllPendingTransactions();
            first_time_save_pendings = false;
        }
        await Promise.all([
            resolve_spending ? Promise.resolve() : dal.addPendingSpentBares(pending_spents_bare),

            resolve_spending ? dal.addPendingSpents(pending_spents) : Promise.resolve(),
            resolve_spending ? dal.addPendingSpentsMultiSig(pending_spents_multisig): Promise.resolve(),
            resolve_spending ? dal.addPendingSpentsNoAddr(pending_spents_noaddr): Promise.resolve(),

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

let packer = function(){
    let max_txs = (typeof config.max_txs === 'undefined') ? 100 : +config.max_txs;
    if(max_txs <= 0) max_txs = 100;

    debug.info(`max_txs = ${max_txs}`);

    let pk_tis; //pack of {height, tis} to process
    let pk_counter; //total txs in pk_tis
    let pk_tail; //tail of pk_tis

    //public:
    return {
        init(){
            pk_tis = [];
            pk_counter = 0;
            pk_tail = { height: -2 }//just start with an invalid height (-1 reserved for pendings)
        },

        async flush(){
            if(pk_counter > 0){
                let N = pk_tis.length;
                if(N > 1){
                    debug.info(`parsing blk #${pk_tis[0].height} to blk (#${pk_tail.height}: ${(pk_tail.count * 100 / pk_tail.total_txs).toFixed(2)}%)...`);
                }else{
                    debug.info(`parsing blk (#${pk_tail.height}: ${(pk_tail.count * 100 / pk_tail.total_txs).toFixed(2)}%)...`);
                }
                await process_tis(pk_tis);

                if(pk_tail.total_txs == pk_tail.count){
                    //no pending tx to the previous block
                    this.init();
                }else{
                    pk_counter = 0;
                    //height & blk_hash & total_txs & last_ti & count kept for more incoming txs of the same block
                    pk_tail.tis = [];
                    pk_tis = [pk_tail];
                }
            }
        },

        /**
         * Cache and process a transaction
         * 
         * @param {int} height - block height 
         * @param {string} blk_hash - block #height's hash 
         * @param {int} total_txs - total transactions in the block
         * @param {obj} ti - transaction info 
         */
        async add_ti(height, blk_hash, total_txs, ti){
            /*
            if(height > 50984){
                await this.flush();
                console.log('DONE!!!');
                debug.throw_error('50984 reached!');
            }*/

            if(pk_tail.height != height){//new block
                pk_tail = {
                    height,
                    blk_hash,
                    total_txs,
                    tis: [ti],
                    count: 1,
                }
                pk_tis.push(pk_tail);
            }else{
                pk_tail.tis.push(ti);
                pk_tail.count++;

                if(pk_tail.count > pk_tail.total_txs) debug.throw_error(`blk #${pk_tail.height}: more tx than expected ${pk_tail.total_txs}!`);
            }
     
            pk_counter++;
            if(pk_counter >= max_txs){
                await this.flush();
            }
        },
    }
}();

async function sample_pendings(){
    debug.info('check pendings >>');

    let txids = await client.getrawmempool();
    debug.info(`Total txs in mem-pool ${txids.length}`);

    if(txids.length > 0){
        let new_txids = []; //new pending txs to save to database.

        if(pending_txids.size > 0){
            txids.forEach(txid => {
                if(!pending_txids.has(txid)){
                    new_txids.push(txid);
                }
            });
        }else new_txids = txids;

        let N = new_txids.length;
        if(N > 0){
            debug.info(`Total new txs in mem-pool ${N}`);
            for(let i = 0; i < new_txids.length; i++){
                const txid = new_txids[i]; 
                let ti = await getTransactionInfo(txid);
                if(ti == null){
                    debug.warn(`transaction [${txid}]  not found, ignore for later processing!`);
                    new_txids[i] = '';
                }else{
                    await packer.add_ti(-1, '', N, ti);
                }
            };

            await packer.flush();

            //now update on success
            new_txids.forEach(txid => {
                if(txid.length > 0) pending_txids.add(txid);
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

/**
 * nStart: block_hash(nStart) == start_blk_hash; (REST)
 * nEnd: next start of batch
 */

async function sample_blocks(nStart, nEnd /* exclude */) {
    let blks = [];

    debug.info(`sync [${nStart}, ${nEnd})...`);

    if(use_rest_api){
        //rest-api
        if(start_blk_hash == null){
            start_blk_hash = await client.getBlockHash(nStart);
        }
        let hdrs = await client.getBlockHeadersByHash(start_blk_hash, nEnd - nStart);
        for(let i = nStart; i < nEnd; i++){
            let blk_hash = hdrs[i-nStart].hash;
            if(latest_block - i < coin_traits.max_confirms){
                blks.push({
                    _id: i,
                    hash: blk_hash
                });
            }

            let blk_info = await client.getBlockByHash(blk_hash);

            if(blk_info.height != i){
                debug.throw_error(`block height mismatch, expected: ${i} got: ${blk_info.height}`);
            }

            let N = blk_info.tx.length; 
            for(const ti of blk_info.tx){
                await packer.add_ti(i, blk_hash, N, ti);
            }
        }
        start_blk_hash = hdrs[nEnd-nStart-1].nextblockhash;
    }else{
        //rpc-api
        for(let i = nStart; i < nEnd; i++){
            let blk_hash = await client.getBlockHash(i);

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
            let N = blk_info.tx.length;
            for(const txid of blk_info.tx){
                let ti = await getTransactionInfo(txid);
                await packer.add_ti(i, blk_hash, N, ti);
            }
        }    
    }

    if(blks.length > 0) await dal.addBackupBlocks(blks);

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

        packer.init();

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
            //debug_throw_error("ROLLBACK detected, PLEASE DEBUG ME!");

            obj.message = 'start';
            await dal.logEvent( obj, 'ROLLBACK', dal.LOG_LEVEL_WARN);

            debug.warn('rollback blockchain...');

            let last_good_block_hash = '';
            let blks = await dal.getBackupBlocks();
            for(let i = blks.length-1; i >= 0; i--){
                let blk = blks[i];
                if(blk._id <= latest_block){
                    let blk_hash = await client.getBlockHash(blk._id);
                    if(blk_hash == blk.hash){
                        last_good_block = blk._id;
                        last_good_block_hash = blk_hash;
                        break;
                    }
                }
            }

            if(last_good_block == -1){
                obj.message = 'failure: not enough backup blocks!';
                await dal.logEvent(cloneObj(obj), 'ROLLBACK', dal.LOG_LEVEL_FATAL);

                debug.throw_error('rollback failure: not enough backup blocks!'); 
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
                if(last_good_block == -1 && last_recorded_blocks >=0) await dal.rollback(last_recorded_blocks);
            }

            let i = last_recorded_blocks + 1; //start blk# of this batch
            if((i == 0) && !coin_traits.genesis_tx_connected) i++; //skip genesis block if its tx is not used.

            let j = i + config.batch_blocks; //start blk# of next batch
            if(j > latest_block) j = latest_block + 1; //last batch

            while(i <= latest_block){
                if(this.stop) break;

                await sample_blocks(i, j);

                i = j;
                j = i + config.batch_blocks;                
                if(j > latest_block) j = latest_block + 1; //last batch
            }
            await packer.flush(); //process partial left items

            postStatus('OK');

            //catch up with latest block first.
            //the latest block maybe changed, so just return for next round.
            //otherwise the pending txs in mempool might refer to unprocessed coins.
        } else {
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
        }

        debug.info('sam.run << ');
    }
}
