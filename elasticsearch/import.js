var elasticsearch = require('elasticsearch');
var csv = require('csv-parser');
var fs = require('fs');

const callsIndex = 'calls';

var esClient = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'error'
});

esClient.indices.delete({
  index: callsIndex
});

esClient.indices
  .create({
    index: callsIndex,
    body: {
      settings: {
        number_of_shards: 1,
        number_of_replicas: 1
      },
      mappings: {
        call: {
          properties: {
            location: {
              type: 'geo_point'
            }
          }
        }
      }
    }
  })
  .catch(error => console.trace(error.message));

let calls = [];

fs.createReadStream('../911.csv')
  .pipe(csv())
  .on('data', data => {
    const { desc, title, twp, addr } = data;
    const cat = title.split(':')[0];
    calls.push({
      cat,
      desc,
      title,
      twp,
      addr,
      location: {
        lat: parseFloat(data.lat),
        lon: parseFloat(data.lng)
      },
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
