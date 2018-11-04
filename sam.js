'use strict'

const common = require('./common');

const debug = common.create_debug('sam');
const config = common.config;
const dal = require('./dal');

const Client = require('bitcoin-core');

//initialized in init();
var client = null; 
var node = null;
var coin_traits = null;

////////////////////////////////////////////////////////////////

async function do_sample(block_no){
    debug.trace(`sampling ${block_no} >>`);

    let blk_hash = await client.getBlockHash(block_no);
    //debug("%s", blk_hash);
    let blk_info = await client.getBlock(blk_hash, coin_traits.getblock_verbose_bool ? true : 1);
    debug.trace("%O", blk_info);

    if(block_no != 0 || coin_traits.genesis_tx_connected){
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
        const N = txs.length;
        if(N > 0){
            for(let i = 0; i < N; i++){
                let txid = txs[i];
                debug.trace('[%d/%d] txid: %s', i, N, txid);

                let tx_info = await client.getRawTransaction(txid, coin_traits.getrawtransaction_verbose_bool ? true : 1);
                /**
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

                let outs = tx_info.vout;
                if(outs.length > 0){
                    let coins = [];
                    let payloads = [];

                    for(let j = 0; j < outs.length; j++){
                        let out = outs[j];
                        if(out.value > 0){//only save non-zero utxo

                            if(out.scriptPubKey.addresses.length != 1){
                                let msg = `UTXO with zero or multiple addresses not supported! blk# [${block_no}] txid [${txid}] pos [${j}]`;
                                debug.fatal(msg);
                                throw new Error(msg);
                            }

                            coins.push({
                                address: out.scriptPubKey.addresses[0],
                                height: block_no,
                                txid: txid,
                                pos: j,
                                value: out.value,
                                
                                spent: false,
                            })
                        }

                        if(out.payloadSize > 0){
                            payloads.push({
                                address: out.scriptPubKey.addresses[0],
                                height: block_no,
                                txid: txid,
                                pos: j,

                                hint: out.payloadHint,
                                subhint: out.payloadSubHint,
                                size: out.payloadSize,
                                payload: out.payload
                            })
                        }
                    }

                    if(coins.length > 0){
                        await dal.addCoins(coins);
                    }

                    if(payloads.length > 0){
                        await dal.addPayloads(payloads);
                    }
                }
            }
        }
    }

    debug.trace(`sampling ${block_no} <<`);
}


////////////////////////////////////////////////////////////////
module.exports = {
    async init(){
        debug.trace('sam.init >> ');

        // full node
        let nodeId = process.env.COIN_NODEID;
        if(nodeId){
            config.nodes.forEach(n=>{
                if(n.id == nodeId) node = n;
            });
        }

        if (node == null) node = config.nodes[0]; //fallover to first node if not specified in config (.env)
        if(node == null)
            throw new Error("full node cannot be resolved!");

        coin_traits = config.coins[node.coin];
        if(typeof coin_traits == 'undefined') throw new Error(`coin_traits for "${node.coin}" not defined!`);
        debug.warn("%O", coin_traits);

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
        debug.trace('sam.run >> ');

        let latest_block = await client.getBlockCount();

        debug.trace(`latest block: ${latest_block}`);

        let last_recorded_blocks = await dal.getLastRecordedBlockHeight();
        debug.trace(`latest recorded block: ${last_recorded_blocks}`);

        if(latest_block > 161) latest_block = 161;

        if(latest_block > last_recorded_blocks){
            for(let i = last_recorded_blocks+1; i <= latest_block; i++){
                await do_sample(i);

                last_recorded_blocks = i;
                await dal.setLastRecordedBlockHeight(i);
            }
        }
        debug.trace('sam.run << ');
    }
}
