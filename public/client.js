function submitQuery() {
   const query = document.getElementById('query').value; // Получаем SQL-запрос из поля ввода
   fetch('/processQuery', {
       method: 'POST',
       headers: {
           'Content-Type': 'application/json'
       },
       body: JSON.stringify({ query }) // Отправляем запрос на сервер
   })
   .then(() => {
       window.location.href = '/result'; // Перенаправляем пользователя на страницу /result
   })
   .catch(error => console.error('Ошибка:', error));
}