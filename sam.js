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

var pending_txids = new Set(); //pending txids

//When it is the first time checking mempool, the pending txs might have been saved to database before
//after that the pending_txids will avoid saving the same pending tx to database.
var first_time_save_pendings = true;

var first_time_check_blocks = true;
////////////////////////////////////////////////////////////////
function throw_error(msg){
    debug.err(msg);
    throw new Error(msg);
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
    let spents = [];
    let coins = [];
    let coins_multisig = [];
    let payloads = [];
    let errs = [];
    let txids = [];

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
                        //Detect if spent the same utxos (previous txs) in the same block.
                        let oldLen = coins.length;
                        //Cancel the (coin, spent) pair in memory
                        for(let m = 0; m < coins.length; m++){
                            let coin = coins[m];
                            if((coin.tx_id == spent.txid) && (coin.pos == spent.vout)){
                                /**
                                 * First matched coin is spent.
                                 * For non-BIP34 compatible cryptocurrency, there might be more than one matched coins, we want only one coin 
                                 * (the first matched) is cancelled, that is why we cannot use the following code:
                                 * 
                                 *  coins = coins.filter(coin => (coin.tx_id != spent.txid)||(coin.pos != spent.vout));
                                 * 
                                 *  it may spend multiple coins with a single spent.
                                 */
                                coins.splice(m, 1);
                                break;
                            }
                        }

                        if(oldLen == coins.length){
                            let obj = {
                                tx_id: txid,
                                spent_tx_id: spent.txid,
                                pos: spent.vout
                            }
                            if(pending){
                                //(address, value) is only needed for pending spents, the coins table only holds unspent xo.
                                //TODO: we can retrieve the (address, value) info from dal --- the coin should already be in database.
                                let tx_spent = await getTransactionInfo(spent.txid);
                                if(tx_spent.vout.length <= spent.vout){
                                    let msg = `Spent UTXO not found, tx_spent.vout.length = [${tx_spent.vout.length}] <= spent.vout [${spent.vout}]`;
                                    debug.fatal("tx_info: %O", tx_info);
                                    debug.fatal("tx_spent: %O", tx_spent);
                                    debug.fatal(msg);
                                    throw new Error(msg);
                                }
                                let out = tx_spent.vout[spent.vout];
                                if(out.scriptPubKey.addresses.length != 1){
                                    let msg = `Spent UTXO with zero or multiple addresses not supported! txid [${spent.txid}] pos [${spent.pos}]`;
                                    debug.fatal(msg);
                                    throw new Error(msg);
                                }
                                obj.address = out.scriptPubKey.addresses[0];
                                let vCoin = new BigNumber(out.value); 
                                obj.value = vCoin.multipliedBy(coin_traits.SAT_PER_COIN).toString();
                            }

                            spents.push(obj);
                        }
                    }
                }                    
            }

            let outs = tx_info.vout;
            if(outs.length > 0){
                for(let j = 0; j < outs.length; j++){
                    let out = outs[j];
                    if(out.value > 0){//only save non-zero utxo
                        if(typeof out.scriptPubKey.addresses === 'undefined'){
                        errs.push({
                            tx_id: txid,
                            height: height,
                            pos: j,
                            message: `non-standard coin detected, no address defined.`,
                            tx_info: tx_info
                        });
                        continue;
                        }

                        let vCoin = new BigNumber(out.value);
                        vCoin = vCoin.multipliedBy(coin_traits.SAT_PER_COIN);

                        let obj = {
                            tx_id: txid,
                            pos: j, //the j'th output in the tx
                            value: vCoin.toString(),
                        };
                        if(height >= 0) obj.height = height; //pending_xxx does not have height field.

                        if(out.scriptPubKey.addresses.length > 1){ //multisig
                            if(out.scriptPubKey.type !== 'multisig' || !coin_traits.MULTISIG)
                                throw_error(`UTXO with zero or multiple addresses not supported! blk# [${height}] txid [${txid}] pos [${j}]`);

                            //MULTISIG support
                            obj.addresses = out.scriptPubKey.addresses;
                            coins_multisig.push(obj);
                        }else{
                            obj.address = out.scriptPubKey.addresses[0];
                            coins.push(obj);
                        }
                    }

                    if(support_payload && (out.payloadSize > 0)){
                        let obj = {
                            address: out.scriptPubKey.addresses[0],
                            tx_id: txid,

                            pos: j, //the j'th output in the tx

                            hint: out.payloadHint,
                            subhint: out.payloadSubHint,
                            size: out.payloadSize,
                            payload: out.payload
                        }
                        
                        if(height >= 0) obj.height = height; //pending_xxx does not have height field.

                        payloads.push(obj);
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
            await dal.removePendingTransactions(txids);
        }

        if(support_payload && (payloads.length > 0)){
            await dal.addPayloads(payloads);
        }

        //addCoins before addSpent! (to support spent uxio of tx in the same block)
        if(coins.length > 0){
            await dal.addCoins(coins);
        }

        if(coins_multisig.length > 0){
            await dal.addMultiSigCoins(coins_multisig);
        }

        if(spents.length > 0){
            await dal.addSpents(spents);
        }
    } else { //pending
        if(first_time_save_pendings){
            await dal.removePendingTransactions(txids);
            first_time_save_pendings = false;
        }
        if(spents.length > 0){
            await dal.addPendingSpents(spents);
        }
        if(coins.length > 0){
            await dal.addPendingCoins(coins);
        }
        if(coins_multisig.length > 0){
            await dal.addPendingMultiSigCoins(coins_multisig);
        }
        if(support_payload && (payloads.length > 0)){
            await dal.addPendingPayloads(payloads);
        }
    }

    //errs
    if(errs.length > 0){
        await dal.addErrors(errs);
    }
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
                    throw_error(`transaction [${txid}]  not found!`);
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

/**
 * nStart: block_hash(nStart) == start_blk_hash; (REST)
 * nEnd: next start of batch
 */

async function sample_batch(nStart, nEnd /* exclude */){
    let blk_tis = [];

    debug.info(`sync [${nStart}, ${nEnd})...`);

    if(use_rest_api){
        //rest-api
        if(start_blk_hash == null){
            start_blk_hash = await client.getBlockHash(nStart);
        }
        let hdrs = await client.getBlockHeadersByHash(start_blk_hash, nEnd - nStart);
        for(let i = nStart; i < nEnd; i++){
            let blk_info = await client.getBlockByHash(hdrs[i-nStart].hash);

            if(blk_info.height != i){
                throw_error(`block height mismatch, expected: ${i} got: ${blk_info.height}`);
            }

            blk_tis.push({
                height: i,
                tis: blk_info.tx //tx_info already populated by REST-api
            });
        }
        start_blk_hash = hdrs[nEnd-nStart-1].nextblockhash;
    }else{
        //rpc-api
        for(let i = nStart; i < nEnd; i++){
            let item = { height: i, tis: [] };

            let blk_hash = await client.getBlockHash(i);
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
    debug.info(`sync [${nStart}, ${nEnd}) done!`);
}
////////////////////////////////////////////////////////////////
module.exports = {
    stop: false, //flag to indicate shut down immediately

    async init(){
        debug.trace('sam.init >> ');

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


        debug.trace('sam.init << ');
    },

    async run(){
        debug.info('sam.run >> ');

        let latest_block = await getLatestBlockCount();

        debug.info(`latest block: ${latest_block}`);

        let last_recorded_blocks = await dal.getLastRecordedBlockHeight();
        debug.info(`latest recorded block: ${last_recorded_blocks}`);

        if(latest_block > last_recorded_blocks){
            if(first_time_check_blocks){
                first_time_check_blocks = false;
                await Promise.all([dal.removeCoinsAfterHeight(last_recorded_blocks), dal.removePayloadsAfterHeight(last_recorded_blocks)]);
            }

            let i = last_recorded_blocks + 1; //start blk# of this batch
            if((i == 0) && !coin_traits.genesis_tx_connected) i++; //skip genesis block if its tx is not used.

            let j = i + config.batch_blocks; //start blk# of next batch
            if(j > latest_block) j = latest_block + 1; //last batch

            while(i <= latest_block){
                if(this.stop) break;

                await sample_batch(i, j);

                await dal.setLastRecordedBlockHeight(j-1);

                i = j;
                j = i + config.batch_blocks;                
                if(j > latest_block) j = latest_block + 1; //last batch
            }
        }

        if(!this.stop){
            //pendings
            await sample_pendings();
        }
        if(!this.stop){
            //rejection
            await check_rejection();
        }

        debug.info('sam.run << ');
    }
}
