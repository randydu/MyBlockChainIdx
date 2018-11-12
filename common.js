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
    
    config.node = node;

    let coin_traits = config.coins[node.coin];
    if(typeof coin_traits == 'undefined') throw new Error(`coin_traits for "${node.coin}" not defined!`);

    config.coin_traits = coin_traits;
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

module.exports = {
    config,
    delay,
    create_debug,
    make_serial,
    add_apis,
}