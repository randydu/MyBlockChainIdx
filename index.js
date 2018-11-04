'use strict'

require('dotenv').config({ path: __dirname + '/.env'});

const sample = require('./sam');

function handle(signal){
    console.log(`signal: ${signal}`);

    process.exit(-1);
}

//-------- ENTRY -------------

process.on('SIGINT', handle);
process.on('SIGTERM', handle);

sample.init().then(()=>{
    const sample_interval = +process.env.SAMPLE_INTERVAL || 1000;
    console.log(`Sampling interval = ${sample_interval}`);
    setInterval(sample.run, sample_interval);
}).catch(err => {
    console.error(err.message);
})

