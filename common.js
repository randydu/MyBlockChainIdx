'use strict'

/**
 * Common Helpers
 */
const fs = require('fs');
const JSON5 = require('json5');
const config = JSON5.parse(fs.readFileSync(__dirname + '/config.json5'));


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