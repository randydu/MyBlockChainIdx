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

 
 //--------------------------
async function run(){
    await dal.init(true);

    let old_ver = await dal.getDBVersion();
    let my_ver = dal.getLatestDBVersion();
    if(old_ver == my_ver){
        console.log("database version matched, no need to upgrade.");
        await dal.close();

        return;
    }

}

return run().then(()=>{
    console.log("done!");
    process.exitCode = 0;

//    debug.info("%O", process._getActiveRequests());
//    debug.info("%O", process._getActiveHandles());
}).catch(err => {
    console.log(err.message);

    process.exitCode = -1;
    return dal.close();
});
