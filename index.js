'use strict'

require('dotenv').config({ path: __dirname + '/.env'});

const common = require('./common');

const sample = require('./sam');
const dal = require('./dal');
const api = require('./api');

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
    if(quit) return Promise.resolve(-1);

    return sample.run().then(r => {
        if(quit) return Promise.resolve(-1);

        return common.delay(sample_interval).then(sample_run);
        //setTimeout(sample_run, sample_interval);
    })
}

async function init(){
    await dal.init();
    await sample.init();
}

if(+process.env.HTTP){
    api.run(process.env.HTTP_PORT);
}

return init().then( sample_run )
    .catch(err => {
        debug.err(err.message);

        return dal.logEvent({
            message: err.message,
            code: 'ERROR',
            level: dal.LOG_LEVEL_ERROR
        }).then(()=>{
            process.exitCode = -1;
            
            api.setStatus(`ERROR: ${err.message}`);

            if(+process.env.EXIT_ON_ERROR){
                process.kill(process.pid, "SIGINT");
            }
        })

    }).then(dal.close);
    
