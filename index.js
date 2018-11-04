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

Promise.all([sample.init(), dal.init()]).then(()=>{
    const sample_interval = +process.env.SAMPLE_INTERVAL || 1000;
    console.log(`Sampling interval = ${sample_interval}`);
    setInterval(sample.run, sample_interval);
}).catch(err => {
    console.error(err.message);
})

