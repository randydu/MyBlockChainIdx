'use strict'


module.exports = {
    status: 'SYNCING',

    run(port){
        const express = require('express');
        const app = express();

        app.get('/status', async (req, res)=>{
            res.send(this.status);
        });
        //Consul service health check
        //2xx: passing, 429: warning, otherwise critical
        app.get('/healthcheck', async (req, res)=> {
            try{
                switch(this.status){
                    case 'OK': return res.status(200).send('OK');
                    case 'SYNCING': return res.status(429).send('SYNCING');
                    default: return res.status(400).send(this.status);
                }
            }catch(err){
                console.error(`check >> ${err.message}`);
                res.status(400).send('ERROR');
            };
        });

        app.listen(port, function(){
            console.log(`API server listening on port ${port}`);
        });
    },

    setStatus(st){
        this.status = st;
    },
}