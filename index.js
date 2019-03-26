'use strict'

require('dotenv').config({ path: __dirname + '/.env'});

var memwatch = null;
const debug_mem_leak_level = +process.env.DEBUG_MEM_LEAK;

const common = require('./common');
const debug = require('mydbg')('index');

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


async function init(){
    await dal.init();
    await sample.init();

    //leakage detection
    //# 0: no detection 1: leak+stats 2: leak+stats+heap_diff
    if(debug_mem_leak_level > 0){
        memwatch = require('node-memwatch');
        
        memwatch.on('leak', function(info){
            debug.warn('%O', info);
        });

        memwatch.on('stats', function(stats){
            debug.info('%O', stats);
        });
    }
}

async function do_sample(){
    
    try {
        await init();

        let to_mem_leak_diff = debug_mem_leak_level == 2;

        while(!quit){
            let hd = to_mem_leak_diff ? new memwatch.HeapDiff() : null;

            await sample.run();
            if(!quit) await common.delay(sample_interval);

            if(hd != null){
                let diff = hd.end();
                debug.info('%O', diff);
            }
        }
    }catch(err){
        debug.err(err.message);

        try {
            debug.info('saving err message to database...');

            await dal.logEvent({
                message: err.message,
                code: 'ERROR',
                level: dal.LOG_LEVEL_ERROR
            });
        }catch(ex){
            debug.err(ex);
        }
        
        process.exitCode = -1;
        
        quit = true; //stop sampling on error, report error status.
        api.setStatus(`ERROR: ${err.message}`);

        if(+process.env.EXIT_ON_ERROR){
            process.kill(process.pid, "SIGINT");
        }
    }finally{
        dal.close();
    }
    return quit ? -1 : 0;
}


if(+process.env.HTTP){
    api.run(process.env.HTTP_PORT);
}

/*
function sample_run(){
    if(quit) return Promise.resolve(-1);

    return sample.run().then(r => {
        if(quit) return Promise.resolve(-1);

        return common.delay(sample_interval).then(sample_run);
        //setTimeout(sample_run, sample_interval);
    })
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
*/

return do_sample();
