// Создаёт на цели репетиции пользователя repl с readWrite только на базе DST_DB:
// репетиция подтверждает, что репликатору и verify этих прав хватает.
// Запуск — mongosh с хоста, под root (пароль root спросит mongosh, пароль repl — скрипт):
//   DST_DB=<база цели> mongosh "mongodb://root@127.0.0.1:27117/admin?directConnection=true" create-user.js
// URI репликатора и verify: mongodb://repl@127.0.0.1:27117/?directConnection=true
const user = 'repl';

const dstDB = process.env.DST_DB;
if (!dstDB) {
  print('задайте DST_DB — базу цели, на которую пишет репликатор');
  quit(2);
}

const admin = db.getSiblingDB('admin');
if (admin.getUser(user)) {
  print(`пользователь ${user} уже есть; пересоздать: db.getSiblingDB('admin').dropUser('${user}') и запустить снова`);
  quit(1);
}

print(`пароль нового пользователя ${user}:`);
admin.createUser({ user, pwd: passwordPrompt(), roles: [{ role: 'readWrite', db: dstDB }] });
print(`создан пользователь ${user}: readWrite на ${dstDB}`);
