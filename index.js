'use strict'

require('dotenv').config({ path: __dirname + '/.env'});

const common = require('./common');
const sample = require('./sam');
const dal = require('./dal');

function handle(signal){
    console.log(`signal: ${signal}`);

    console.log('shutdown ...');
    dal.close();

    common.delay(5000);
    console.log('shutdown done!');
    process.exit(-1);
}

//-------- ENTRY -------------

process.on('SIGINT', handle);
process.on('SIGTERM', handle);

const sample_interval = +process.env.SAMPLE_INTERVAL || 1000;
console.log(`Sampling interval = ${sample_interval}`);

function sample_run(){
    return sample.run().then(r => {
        setTimeout(sample_run, sample_interval);
    })
}

return Promise.all([sample.init(), dal.init()]).then(()=>{
   // return sample.run();
   return sample_run();
}).catch(err => {
    console.error(err.message);
})

