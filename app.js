const express = require('express')
const app = express()
const bodyParser = require('body-parser')
const read = require('node-readability')
const Database = require('./database.js').Database;
app.use(express.static('public'));




app.set('port', 3000)
app.use(bodyParser.json()) //парсер поддерживает теперь тела запросов в формате json
app.use(bodyParser.urlencoded({extended:true}))//поддерживает в кодировке формы


const usersSql = `SELECT * FROM users LIMIT 15 OFFSET 0`
const tweetsSql = `SELECT * FROM tweets LIMIT 15 OFFSET 0`
const hashtagsSql = `SELECT * FROM hashtags LIMIT 15 OFFSET 0`
const hashtags_tweetsSql = `SELECT * FROM hashtags_tweets LIMIT 15 OFFSET 0`
const mentionsSql = `SELECT * FROM mentions LIMIT 15 OFFSET 0`
const attachmentsSql = `SELECT * FROM attachments LIMIT 15 OFFSET 0`
const urlsSql = `SELECT * FROM urls LIMIT 15 OFFSET 0`
const repliesSql = `SELECT * FROM replies LIMIT 15 OFFSET 0`



app.get('/users', (req,res,next)=>{//рендер html
   Database.all(usersSql,(err,rows)=>{
      if(err) return next(err)


      const columns = Object.keys(rows[0]); // Получить названия колонок из первой строки
      const data = rows.map(row => Object.values(row)); // Преобразовать объекты в массивы

      res.format({
         html: () => {
            res.render('table.ejs', { columns, data });
         },
         json: () => {
            res.send(rows);
         }
      });
   })
})
app.get('/tweets', (req,res,next)=>{//рендер html
   Database.all(tweetsSql,(err,rows)=>{
      if(err) return next(err)


      const columns = Object.keys(rows[0]); // Получить названия колонок из первой строки
      const data = rows.map(row => Object.values(row)); // Преобразовать объекты в массивы

      res.format({
         html: () => {
            res.render('table.ejs', { columns, data });
         },
         json: () => {
            res.send(rows);
         }
      });
   })
})
app.get('/hashtags', (req,res,next)=>{//рендер html
   Database.all(hashtagsSql,(err,rows)=>{
      if(err) return next(err)


      const columns = Object.keys(rows[0]); // Получить названия колонок из первой строки
      const data = rows.map(row => Object.values(row)); // Преобразовать объекты в массивы

      res.format({
         html: () => {
            res.render('table.ejs', { columns, data });
         },
         json: () => {
            res.send(rows);
         }
      });
   })
})
app.get('/hashtags_tweets', (req,res,next)=>{//рендер html
   Database.all(hashtags_tweetsSql,(err,rows)=>{
      if(err) return next(err)


      const columns = Object.keys(rows[0]); // Получить названия колонок из первой строки
      const data = rows.map(row => Object.values(row)); // Преобразовать объекты в массивы

      res.format({
         html: () => {
            res.render('table.ejs', { columns, data });
         },
         json: () => {
            res.send(rows);
         }
      });
   })
})
app.get('/attachments', (req,res,next)=>{//рендер html
   Database.all(attachmentsSql,(err,rows)=>{
      if(err) return next(err)


      const columns = Object.keys(rows[0]); // Получить названия колонок из первой строки
      const data = rows.map(row => Object.values(row)); // Преобразовать объекты в массивы

      res.format({
         html: () => {
            res.render('table.ejs', { columns, data });
         },
         json: () => {
            res.send(rows);
         }
      });
   })
})
app.get('/urls', (req,res,next)=>{//рендер html
   Database.all(urlsSql,(err,rows)=>{
      if(err) return next(err)


      const columns = Object.keys(rows[0]); // Получить названия колонок из первой строки
      const data = rows.map(row => Object.values(row)); // Преобразовать объекты в массивы

      res.format({
         html: () => {
            res.render('table.ejs', { columns, data });
         },
         json: () => {
            res.send(rows);
         }
      });
   })
})
app.get('/mentions', (req,res,next)=>{//рендер html
   Database.all(mentionsSql,(err,rows)=>{
      if(err) return next(err)


      const columns = Object.keys(rows[0]); // Получить названия колонок из первой строки
      const data = rows.map(row => Object.values(row)); // Преобразовать объекты в массивы

      res.format({
         html: () => {
            res.render('table.ejs', { columns, data });
         },
         json: () => {
            res.send(rows);
         }
      });
   })
})
app.get('/replies', (req,res,next)=>{//рендер html
   Database.all(repliesSql,(err,rows)=>{
      if(err) return next(err)


      const columns = Object.keys(rows[0]); // Получить названия колонок из первой строки
      const data = rows.map(row => Object.values(row)); // Преобразовать объекты в массивы

      res.format({
         html: () => {
            res.render('table.ejs', { columns, data });
         },
         json: () => {
            res.send(rows);
         }
      });
   })
})


let savedQuery = ''; // Переменная для хранения запроса

app.post('/processQuery', (req, res) => {
    const { query } = req.body; // Получаем SQL-запрос от клиента
    savedQuery = query +" LIMIT 15 OFFSET 0"; // Сохраняем запрос в переменной для дальнейшей обработки
    res.send('Query received on the server'); // Отправляем ответ обратно на клиентскую сторону
});
app.get('/result', (req,res,next)=>{//рендер html
   Database.all(savedQuery,(err,rows)=>{
      if(err) return next(err)


      if (rows && rows.length > 0) {
         const columns = Object.keys(rows[0]);
         const data = rows.map(row => Object.values(row));

         res.format({
            html: () => {
               res.render('table.ejs', { columns, data });
            },
            json: () => {
               res.send(rows);
            }
         });
      } else {
         // Обработка ситуации, когда результаты запроса пусты
         res.send('No data found');
      }
   })
})










app.listen(app.get('port'), ()=>{
   console.log(`web app is active at http://127.0.0.1:${app.get('port')}`)
})

module.exports = app