/**
 * Convert int32-indexed old database to int64-indexed new database
 * 
 */

'use strict';

require('dotenv').config({ path: __dirname + '/.env'});

const common = require('./common');
const debug = common.create_debug('up');
const dbg_throw_error = common.dbg_throw_error(debug);

const config = common.config;

const dal = require('./dal');

async function handle(signal){
    console.log(`signal: ${signal}`);

    console.log('shutdown ...');
    dal.stop();

    await common.delay(5000);
    await dal.close();
    console.log('shutdown done!');
    process.exit(-1);
}
 //--------------------------
async function run(){
    await dal.init(true);

    let old_ver = await dal.getDBVersion();
    let my_ver = dal.getLatestDBVersion();
    if(old_ver == my_ver){
        console.log("database version matched, no need to upgrade.");
        return;
    }

    if((old_ver == 1) && (my_ver == 2)){
        await dal.upgradeV1toV2(debug);
    } else if((old_ver == 2) && (my_ver == 3)){
        await dal.upgradeV2toV3(debug);
    } else {
        dbg_throw_error(`Version upgrade ${old_ver}=>${my_ver} not implemented!`);
    }
}

process.on('SIGINT', handle);
process.on('SIGTERM', handle);

return run().then(()=>{
        console.log("done!");
        process.exitCode = 0;

        //    debug.info("%O", process._getActiveRequests());
        //    debug.info("%O", process._getActiveHandles());
    }).catch(err => {
        console.log(err.message);
        process.exitCode = -1;
    }).then(dal.close)
