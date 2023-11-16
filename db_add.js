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
      const insertUserStmt = db.prepare('INSERT OR IGNORE INTO users (id) VALUES(?)');
      const insertHashtagStmt = db.prepare('INSERT OR IGNORE INTO hashtags (title) VALUES (?)');
      const insertHashtagTweetStmt = db.prepare('INSERT OR IGNORE INTO hashtags_tweets (tweet_id, hashtag_id) VALUES (?, ?)');
      const insertUrlStmt = db.prepare('INSERT OR IGNORE INTO urls (short_link, extended_link) VALUES (?, ?)');
      const insertMentionStmt = db.prepare('INSERT OR IGNORE INTO mentions (user_id, tweet_id) VALUES (?, ?)');
      const insertAttachmentStmt = db.prepare('INSERT OR IGNORE INTO attachments (id, title, type, path) VALUES(?, ?, ?, ?)');
      const insertReplyStmt = db.prepare('INSERT OR IGNORE INTO replies (tweet_id, in_reply_to_user_id, in_reply_to_tweet_id) VALUES (?, ?, ?)');
      const insertTweetStmt = db.prepare('INSERT OR IGNORE INTO tweets (id, user_id, text, attachments_id, urls_id, likes, replies_count, retweets_count, created_at, platform) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
      const insertUserMention = db.prepare('INSERT OR IGNORE INTO users (id, screen_name, name) VALUES(?,?,?)')
      const selectStmt = db.prepare('SELECT id FROM hashtags WHERE title = ?');

         rows.forEach(row => {
               const parsedRow = JSON.parse(row.result);
               const tweets = parsedRow.globalObjects.tweets;

               for (const tweetId in tweets) {
                  if (tweets.hasOwnProperty(tweetId)) {
                     var tweet = tweets[tweetId];
                  

                  const post = {
                  id: tweet.id,
                  created_at: tweet.created_at,
                  full_text: tweet.full_text,
                  entities: tweet.entities,
                  in_reply_to_status_id: tweet.in_reply_to_status_id,
                  in_reply_to_user_id: tweet.in_reply_to_user_id,
                  user_id: tweet.user_id,
                  retweet_count: tweet.retweet_count,
                  favorite_count: tweet.favorite_count,
                  reply_count: tweet.reply_count,
                  source: tweet.source,
                  };
         
         
                     
                  insertUserStmt.run(post.user_id, err=>{
                  if(err){
                     console.error('ошибка при вставке в users',err)
                     }else{
                     console.log('успешная вставка в users')
                     }
                  })
               
                  if (post.entities.hashtags) {
                  post.entities.hashtags.forEach(hashtag => {
                     const hashtagExists = db.get('SELECT id FROM hashtags WHERE title = ?', hashtag.text.id);
                     if(!hashtagExists){
                        
                        insertHashtagStmt.run(hashtag.text, err => {
                           if (err) {
                           console.error('Ошибка при вставке в базу данных:', err);
                        } else {
                           console.log('Данные успешно вставлены в таблицу hashtags');
                        }
                        
                     });
                  }
                  
                  
                  selectStmt.get(hashtag.text, (err, row) => {
                  if (err) {
                  console.error('Ошибка при выполнении SELECT:', err);
                  } else {
                     const hashtag_id = post.id;
                     insertHashtagTweetStmt.run(post.id, hashtag_id, err => {
                        if (err) {
                           console.error('Ошибка при вставке в таблицу hashtags_tweets:', err);
                        } else {
                           console.log('Данные успешно вставлены в таблицу hashtags_tweets');
                        }
                        
                     });
                  }
                  });
               
               });
         
                  

               var urlsIds = []
               if (post.entities.urls) {
               post.entities.urls.forEach(url => {
                     
                  insertUrlStmt.run(url.url, url.expanded_url, err => {
                  if (err) {
                  console.error('Ошибка при вставке в базу данных:', err);
                  } else {
                  console.log('Данные успешно вставлены в таблицу urls');
                  }
                  
               });
               const lastInsertedId = insertUrlStmt.lastID;
               urlsIds.push(lastInsertedId);
               });
               }

               if (post.entities.user_mentions) {
               post.entities.user_mentions.forEach(mention => {
                  
                  insertMentionStmt.run(mention.id, post.id, err => {
                     if (err) {
                     console.error('Ошибка при вставке в базу данных mentions', err);
                     } else {
                     console.log('Данные успешно вставлены в таблицу mentions');
                     }
                     
                  });
                  
                     const userExists = db.get('SELECT id FROM users WHERE id = ?', mention.id);

                     if (!userExists) {
                        
                        insertUserMention.run(mention.id, mention.screen_name, mention.name, err => {
                        if (err) {
                           console.error('Ошибка при вставке в таблицу users', err);
                        } else {
                           console.log('Данные успешно вставлены в таблицу users');
                        }
                        
                        });
                     }
                  });
               }      
            
               var mediaIds = []
               if(post.entities.media){
               post.entities.media.forEach(media=>{
                  
                  insertAttachmentStmt.run(media.id, media.url, media.type, media.media_url, err=>{
                     if (err){
                        console.error('ошибка при вставке в таблицу attachments', err);
                     } else{
                        console.log('успешная вставка в таблицу attachments');
                     }
                     
                  });
                  mediaIds.push(media.id)
               });
               }

               if(post.in_reply_to_user_id && post.in_reply_to_status_id){
               
                  insertReplyStmt.run(post.id, post.in_reply_to_user_id, post.in_reply_to_status_id, err=>{
                  if(err){
                     console.error('ошибка при вставке в таблицу replies', err)
                  } else{
                     console.log('успешная вставка в таблицу replies')
                  }
               })
               
               }

            
               const regex = /Twitter for (\w+)/;
               const match = post.source.match(regex);
               let sourceText;
               if (match) {
               sourceText = match[0];
               }
               insertTweetStmt.run(post.id, post.user_id, post.full_text, mediaIds, urlsIds, post.favorite_count, post.reply_count, post.retweet_count, post.created_at, sourceText, err=>{
                  if(err){
                     console.error('ошибка вставки в tweets',err)
                  }else{
                     console.log('успешная вставка в tweets')
                  }
               })
               
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
         insertTweetStmt.finalize()
         insertReplyStmt.finalize();
         insertUserMention.finalize();
         insertMentionStmt.finalize();
         insertAttachmentStmt.finalize();
         insertUrlStmt.finalize();
         selectStmt.finalize();
         insertHashtagTweetStmt.finalize();
         insertHashtagStmt.finalize(); 
      }
      
   });
   
   
}

processNextBatch();







   
  

 
