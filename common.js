'use strict'

/**
 * Common Helpers
 */
const fs = require('fs');
const JSON5 = require('json5');
const config = JSON5.parse(fs.readFileSync(__dirname + '/config.json5'));

function resolve_config(){
    // full node
    let node = null;
    let nodeId = process.env.COIN_NODEID;
    if(nodeId){
        config.nodes.forEach(n=>{
            if(n.id == nodeId) node = n;
        });
    }

    if (node == null) node = config.nodes[0]; //fallover to first node if not specified in config (.env)
    if(node == null)
        throw new Error("full node cannot be resolved!");
    
    //override node settings by env
    node.rpcport = +process.env.RPC_PORT || node.rpcport;
    node.rpchost = process.env.RPC_HOST || node.rpchost;
    node.rpcuser = process.env.RPC_USER || node.rpcuser;
    node.rpcpassword = process.env.RPC_PASSWORD || node.rpcpassword;

    config.node = node;

    let coin_traits = config.coins[node.coin];
    if(typeof coin_traits == 'undefined') throw new Error(`coin_traits for "${node.coin}" not defined!`);

    config.coin_traits = coin_traits;

    //use REST API?
    config.use_rest_api = +process.env.USE_REST_API;
    if(!coin_traits.REST) config.use_rest_api = 0;

    //Batch Size
    config.batch_blocks = + process.env.BATCH_BLOCKS || +config.batch_blocks;
    if(config.batch_blocks < 1) config.batch_blocks = 1;

    //Batch upgrade size
    config.batch_upgradeV1toV2 = + process.env.BATCH_UPGRADEV1TOV2 || 100;
    if(config.batch_upgradeV1toV2 < 1) config.batch_upgradeV1toV2 = 100;
}

resolve_config();


const delay = (duration) => new Promise(resolve => setTimeout(resolve, duration));

function create_debug(name){
    const dbg = require('debug');
    return {
        trace: dbg(`${name}.trace`),
        info: dbg(`${name}.info`),
        warn: dbg(`${name}.warn`),
        err: dbg(`${name}.err`),
        fatal: dbg(`${name}.fatal`)
    };
}

//returns a promise executing input tasks (promises) in serial.
function make_serial(tasks){
    return tasks.reduce((chain, currentTask) => {
        return chain.then(currentTask);
    }, Promise.resolve());
}

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }
var _lodash = _interopRequireDefault(require("lodash"));

function add_apis(client, apis){
    apis.forEach(method => {
        client[method] = _lodash.default.partial(client.command, method.toLowerCase());
    })
}

function throw_error(dbg, msg){
    dbg.err(msg);
    throw new Error(msg);
}

module.exports = {
    config,
    delay,
    create_debug,
    make_serial,
    add_apis,
    dbg_throw_error(dbg){
        return throw_error.bind(dbg);
    }
}