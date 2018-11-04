'use strict'

require('dotenv').config({ path: __dirname + '/.env'});

function do_sample(){
    console.log('sampling...');
}

function handle(signal){
    console.log(`signal: ${signal}`);

    process.exit(-1);
}

//-------- ENTRY -------------

process.on('SIGINT', handle);
process.on('SIGTERM', handle);

const sample_interval = +process.env.SAMPLE_INTERVAL || 1000;
console.log(`Sampling interval = ${sample_interval}`);
setInterval(do_sample, sample_interval);

