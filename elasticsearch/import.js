var elasticsearch = require('elasticsearch');
var csv = require('csv-parser');
var fs = require('fs');

const callsIndex = 'calls';

var esClient = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'error'
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
            },
            cat: {
              type: 'keyword'
            },
            type: {
              type: 'text',
              fielddata: true
            },
            date: {
              type: 'date',
              format: 'yyyy-MM-dd HH:mm:ss'
            },
            twp: {
              type: 'keyword'
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
    const { desc, title, twp, addr, timeStamp } = data;
    const cat = title.split(':')[0];
    const type = title.slice(cat.length + 2); // +2 to remove the space after ':'
    calls.push({
      cat,
      desc,
      type,
      twp,
      addr,
      date: timeStamp,
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
