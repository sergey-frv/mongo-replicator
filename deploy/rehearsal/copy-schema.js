// Копирует схему базы источника на пустую цель репетиции: коллекции с опциями,
// индексы, views. Данные не читаются. Результат проверяет
// mongo-replicator verify --schema-only.
// Запуск — mongosh с хоста:
//   SRC_URI=... DST_URI=... DB=<база источника> DST_DB=<база цели> mongosh --nodb --quiet copy-schema.js
// DST_DB по умолчанию — как DB. Пользователь в URI без пароля — пароль спросит скрипт.
// Цель должна быть пустой: если в DST_DB уже есть коллекции, скрипт ничего не делает.

// служебная коллекция репликатора (internal/replicator: ckptColl)
const ckptColl = '_replicator_checkpoint';

function env(name, def) {
  const v = process.env[name] || def;
  if (!v) {
    print(`задайте ${name}`);
    quit(2);
  }
  return v;
}

// withPassword вставляет в URI пароль с терминала, если пользователь в нём есть, а пароля нет.
function withPassword(side, uri) {
  const m = uri.match(/^(mongodb(?:\+srv)?:\/\/)([^@/]+)@(.*)$/);
  if (!m || m[2].includes(':')) {
    return uri;
  }
  print(`пароль ${side} (${decodeURIComponent(m[2])}):`);
  return `${m[1]}${m[2]}:${encodeURIComponent(passwordPrompt())}@${m[3]}`;
}

// withoutCollationVersion — version в collation задаёт сервер (версия ICU), с другой
// версией ICU цель отвергла бы её
function withoutCollationVersion(doc) {
  if (doc.collation) {
    delete doc.collation.version;
  }
  return doc;
}

// fail печатает, на чём остановилась копия, и выходит: что создано до этого,
// осталось на цели — перед повтором её нужно очистить.
function fail(what, e) {
  print(`ошибка: ${what}: ${e.message}`);
  print('копия схемы остановлена; созданное выше осталось на цели — удалите базу цели перед повтором');
  quit(1);
}

const dbName = env('DB');
const dstName = env('DST_DB', dbName);
const src = new Mongo(withPassword('источника', env('SRC_URI'))).getDB(dbName);
const dst = new Mongo(withPassword('цели', env('DST_URI'))).getDB(dstName);

const existing = dst.getCollectionInfos({}, { nameOnly: true }).map((c) => c.name);
if (existing.length > 0) {
  print(`цель ${dstName} не пустая (${existing.join(', ')}): схема копируется только на пустую базу`);
  quit(1);
}

const infos = src
  .getCollectionInfos()
  .filter((c) => !c.name.startsWith('system.') && c.name !== ckptColl)
  .sort((a, b) => (a.name < b.name ? -1 : 1));
const unknown = infos.filter((c) => c.type !== 'collection' && c.type !== 'view');
if (unknown.length > 0) {
  print(`неизвестный тип: ${unknown.map((c) => `${c.name} (${c.type})`).join(', ')}`);
  quit(1);
}
const colls = infos.filter((c) => c.type === 'collection');
let views = infos.filter((c) => c.type === 'view');

// 1. коллекции с опциями из listCollections как есть
for (const c of colls) {
  const opts = withoutCollationVersion(c.options);
  try {
    dst.createCollection(c.name, opts);
  } catch (e) {
    fail(`коллекция ${c.name}`, e);
  }
  print(`коллекция ${c.name} ${EJSON.stringify(opts)}`);
}

// 2. индексы со всеми полями спецификации, кроме v и ns; _id_ создала сама коллекция
for (const c of colls) {
  const specs = src
    .getCollection(c.name)
    .getIndexes()
    .filter((ix) => ix.name !== '_id_')
    .map((ix) => {
      delete ix.v;
      delete ix.ns;
      return withoutCollationVersion(ix);
    });
  if (specs.length === 0) {
    continue;
  }
  try {
    const res = dst.runCommand({ createIndexes: c.name, indexes: specs });
    if (!res.ok) {
      throw new Error(res.errmsg);
    }
  } catch (e) {
    fail(`индексы ${c.name}`, e);
  }
  for (const ix of specs) {
    print(`индекс ${c.name}.${ix.name} ${EJSON.stringify(ix)}`);
  }
}

// 3. views: view над другой view — после неё
while (views.length > 0) {
  const pending = new Set(views.map((v) => v.name));
  const ready = views.filter((v) => !pending.has(v.options.viewOn));
  if (ready.length === 0) {
    print(`views ссылаются друг на друга по кругу: ${[...pending].join(', ')}`);
    quit(1);
  }
  for (const v of ready) {
    const opts = withoutCollationVersion(v.options);
    try {
      dst.createCollection(v.name, opts);
    } catch (e) {
      fail(`view ${v.name}`, e);
    }
    print(`view ${v.name} ${EJSON.stringify(opts)}`);
  }
  views = views.filter((v) => !ready.includes(v));
}

print(`схема ${dbName} скопирована в ${dstName}: коллекций ${colls.length}, views ${infos.length - colls.length}`);
