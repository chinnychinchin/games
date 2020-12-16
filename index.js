//Load libraries
const express = require('express');
const mysql2 = require('mysql2/promise');
const { MongoClient } = require('mongodb');


//Create MySql pool 
const pool = mysql2.createPool({

    port: process.env.MYSQL_PORT || 3306,
    host: process.env.MYSQL_HOST || "localhost",
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DB || "bgg",
    connectionLimit: 4,
    timezone: "+08:00"
})


//Configure MongoDb 
const MONGO_URL = "mongodb://localhost:27017";
const mongoClient = new MongoClient(MONGO_URL, {useNewUrlParser: true, useUnifiedTopology: true})
const MONGO_DB = 'bgg';
const MONGO_COL = 'bgg_col';

//mySQL queries
const SQL_GET_GAME_BY_ID = "select name, year, url, image from game where gid = ?"


//MongoDb queries
const getRatings = (id, client) => {

    return client.db(MONGO_DB).collection(MONGO_COL).aggregate([
        {
            $match: {ID: id}
        },
         
        {$group: {
            _id: "$ID",
            ratings: {$push: "$rating"},
            reviews: {$push: "$_id"}
                 
                 } 
            
        },
        {
            $project: {_id: 0, avg_rating: {$avg: "$ratings"}, reviews: 1 }
        }
       
     ]).toArray()

}



//Instantiate express
const app = express();

//Start application 
const p0 = pool.getConnection();
const p1 = mongoClient.connect();
const PORT = parseInt(process.argv[2]) || parseInt(process.env.PORT) || 3000;

Promise.all([p0,p1]).then(result => {

    const conn = result[0];
    console.log("Connected to MySQL and MongoDB");
    app.listen(PORT, () => {console.log(`App started on port ${PORT} at ${new Date()}`)})
    conn.release()
    
}).catch(e => {console.log("Unable to ping databases.", e)})


app.get('/game/:id', async (req, res) => {

    const id = req.params['id'];
    const conn = await pool.getConnection();
    try{
        const [result,_] = await conn.query(SQL_GET_GAME_BY_ID, id);
        const mongoRes = await getRatings(parseInt(id), mongoClient);
        //console.log(mongoRes)
        //console.log(result)
        if(!result.length)
        {
            throw new Error("Game not found.")
        }
        else{
            res.status(200).type('application/json').json({name: result[0].name, year: result[0].year, url: result[0].url, image: result[0].image, reviews: mongoRes[0].reviews, avg_rating: mongoRes[0].avg_rating})
        } 
        
    }
    catch(e){
        res.status(404).type('application/json').json({e: e.message})
    }
    finally{
        conn.release()
    }


})