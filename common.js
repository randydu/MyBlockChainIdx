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

module.exports = {
    config,
    delay,
    create_debug,
}