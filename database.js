const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('db.sqlite3');

class Database{
   static all(query, cb){
      db.all(query,cb)
   }
   // static printData(tableName) {
   //    return new Promise((resolve, reject) => {
   //       const sql = `SELECT * FROM ${tableName} LIMIT 5 OFFSET 0`;
   //       var data = []
   //       var columns = [];

   //       db.all(sql, (err, rows) => {
   //          if (err) {
   //             reject(err);
   //          } else {
   //             rows.forEach(row => {
   //                columns = Object.keys(row);
   //                data.push(row);
   //             });
   //             resolve({ data, columns });
   //          }
   //       });
   //    });
   // }
   // static execQuery(query,cb){
   //    db.run(query,cb);
   // }
}


module.exports = db;
module.exports.Database = Database;
