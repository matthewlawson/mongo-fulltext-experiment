if(process.argv[2] === undefined) {
  console.error('Usage: `npm start SEARCH_QUERY`')
  process.exit();
}

const MongoClient = require('mongodb').MongoClient;
const restaurantData = require('./restaurants.json')
const url = 'mongodb://localhost:27017';
 
// Database Name
const dbName = 'Restaurant-spike';
// Use connect method to connect to the server
MongoClient.connect(url, function(err, client) {
  
  const db = client.db(dbName);
  indexCollection(db, () => {
    console.log("index created");
    insertData(db, () => {
      console.log("data inserted");

      queryRestaurants(db, () => {

      });
      client.close();
    });
  });
});

/**
 * Create fulltext index on all fields containing text and geo index.
 * @param {*} db 
 * @param {*} callback 
 * https://docs.mongodb.com/manual/tutorial/control-results-of-text-search/
 */
const indexCollection = (db, callback) => {
  // https://docs.mongodb.com/manual/reference/command/createIndexes/
  db.collection('restaurants').createIndexes(
    [
      { "key": { "geo": "2dsphere" } },
      { "key": { 
          "name": "text",
          "address.building": "text",
          "address.town": "text",
          "address.street": "text",
          "address.county": "text",
          "address.postcode": "text",
          "address.country": "text"
        },
        // Everything else defaults to 1
        "weights": {
          "name": 10,
          "address.town": 5,
          "address.county": 4
        }, 
        "default_language": "english",
        "name": "TextIndex"
      },
      { "key": { "id": 1}, "unique": true}
    ],
    {},
    (err, result) => {
      console.log(err, result);
      callback();
    }
  );
};

const insertData = (db, callback)  => {
  // console.log(restaurantData);
  restaurantData.forEach(element => {
    db.collection('restaurants').replaceOne(
      {id: element.id},
      element,
      {upsert: true}
    )
  });
  callback();
}

const queryRestaurants = (db, callback) => {
  const cursor = 
    db.collection('restaurants')
      .find( 
        {
          '$text': {
            '$search': process.argv[2] 
          },
          "geo": { $geoWithin: { $center: [ [-0.043945, 51.482452], 1 ] } } 
        },
        { 
          'projection': {'score': { $meta: "textScore" } }
        } 
      )
      .sort({ score: { $meta: "textScore" } })
      .limit(10);

  cursor.toArray((err, docs) => {
    console.log(err, docs);
    callback(docs);
  });
};

