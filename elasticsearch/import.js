var elasticsearch = require('elasticsearch');
var csv = require('csv-parser');
var fs = require('fs');

const callsIndex = 'calls';

var esClient = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'error'
});

esClient.indices.delete(
  {
    index: callsIndex
  },
  error => {
    if (error) console.trace(error.message);
  }
);

esClient.indices.create(
  {
    index: callsIndex
  },
  error => {
    if (error) console.trace(error.message);
  }
);

let calls = [];

fs.createReadStream('../911.csv')
  .pipe(csv())
  .on('data', data => {
    delete data.e;
    calls.push({
      ...data,
      lat: parseFloat(data.lat),
      lng: parseFloat(data.lng),
      zip: parseInt(data.zip)
    });
  })
  .on('end', () => {
    esClient.bulk(bulkQuery(calls), error => {
      if (error) console.trace(error.message);
      else console.log(`Inserted ${calls.length} calls`);
      esClient.close();
    });
  });

bulkQuery = calls => {
  const body = calls.reduce((acc, call) => {
    acc.push({
      index: {
        _index: callsIndex,
        _type: 'call'
      }
    });
    acc.push(call);
    return acc;
  }, []);
  return { body };
};
