const sqlite3 = require('sqlite3').verbose()

const sourceDb = new sqlite3.Database('twiiter_tag.sqlite3');
const db = new sqlite3.Database('db.sqlite3');


const limit = 10; 
let offset = 0;    

function processNextBatch() {
   
   sourceDb.all(`SELECT * FROM covid_19 LIMIT ${limit} OFFSET ${offset}`, (err, rows) => {
      if (err) {
         console.error('Ошибка при выполнении запроса:', err);
         return;
      }
      const insertHashtagStmt = db.prepare('INSERT OR IGNORE INTO hashtags (title) VALUES (?)');

      rows.forEach(row => {
         const parsedRow = JSON.parse(row.result);
         const tweets = parsedRow.globalObjects.tweets;

         for (const tweetId in tweets) {
            if (tweets.hasOwnProperty(tweetId)) {
               var tweet = tweets[tweetId];

               const hashtags = tweet.entities.hashtags;
               if (hashtags) {
                  
                  hashtags.forEach(hashtag => {
                     const hashtagExists = db.get('SELECT id FROM hashtags WHERE title = ?', hashtag.text, (err, row) => {
                        if (err) {
                           console.error('Ошибка при выполнении запроса:', err);
                        } else {
                           if (!row) {
                              insertHashtagStmt.run(hashtag.text, err => {
                                 if (err) {
                                    console.error('Ошибка при вставке в базу данных:', err);
                                 } else {
                                    console.log('Данные успешно вставлены в таблицу hashtags');
                                 }
                           
                              });
                           }
                        }
                     });
                     
                        
                        
                     

                  });
               }
            }
         }
         offset += 1;
      });
      if (rows.length === limit) {
         processNextBatch();
         } else {
            db.close();
            sourceDb.close();
            insertHashtagStmt.finalize(); 
         }
   });
}
processNextBatch();
