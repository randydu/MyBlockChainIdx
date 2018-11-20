'use strict'

require('dotenv').config({ path: __dirname + '/.env'});

const common = require('./common');
const sample = require('./sam');
const dal = require('./dal');

let quit = false;

async function handle(signal){
    console.log(`signal: ${signal}`);

    console.log('shutdown ...');
    quit = true;
    sample.stop = true;

    await common.delay(5000);
    await dal.close();
    console.log('shutdown done!');
    process.exit(-1);
}

//-------- ENTRY -------------

process.on('SIGINT', handle);
process.on('SIGTERM', handle);

const sample_interval = +process.env.SAMPLE_INTERVAL || 1000;
console.log(`Sampling interval = ${sample_interval}`);

function sample_run(){
    if(quit) return;

    return sample.run().then(r => {
        setTimeout(sample_run, sample_interval);
    })
}

async function init(){
    await dal.init();
    await sample.init();
}

return init().then(()=>{
   // return sample.run();
   return sample_run();
}).catch(err => {
    console.error(err.message);

    process.exit(-1);
})

