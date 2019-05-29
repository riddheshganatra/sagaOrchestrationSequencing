var express = require('express');        // call express
var app = express();                 // define our app using express
var bodyParser = require('body-parser');
const kafka = require('kafka-node');
var uuid = require('node-uuid');
const Producer = kafka.Producer;
const client = new kafka.KafkaClient(`localhost:2181`);
const producer = new Producer(client);
const Consumer = kafka.Consumer;

// roll back transaction
// timeout
// scale
// kakfa is down from mid way


app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

var port = process.env.PORT || 8080;

// to store response object
let requestIds = {};

var router = express.Router();

router.get('/', async function (req, res) {
  let reqID = uuid.v1();

  try {
    requestIds[reqID] = {
      resObject: res
    }

    // verify-consumer
    await productMessage(`verify-consumer`, `verifying consumer`, 1, reqID, req.query.message);

  } catch (error) {

  }


});

app.use('/api', router);

app.listen(port);
console.log('Magic happens on port ' + port);



// reply channel

let consumer = new Consumer(
  client,
  [{ topic: `order-reply-channel`, partition: 0 }],
  {
    autoCommit: true,
    fetchMaxWaitMs: 1000,
    fetchMaxBytes: 1024 * 1024,
    encoding: 'utf8',
    fromOffset: false
  }
);
consumer.on('message', async function (message) {

  let temp = JSON.parse(message.value);
  console.log(`${temp.reqID}:current state: ${temp.state}`);

  switch (temp.state) {
    case `consumer verified`:
      // update state
      await productMessage(`create-ticket`, `creating ticket`, 1, temp.reqID, temp.message);
      break;

    case `consumer verification failed`:
      requestIds[temp.reqID].resObject.send({ message: 'consumer verification failed' })
      break;

    case `ticket created`:
      // send response back
      requestIds[temp.reqID].resObject.send({ message: temp.message })
      delete requestIds[temp.reqID];
      break;

    case `ticket creation failed`:

      await productMessage(`verify-consumer`, `rollback:verifying consumer`, 1, temp.reqID, temp.message);


      break;

    case `verifying consumer rollbacked`:
      requestIds[temp.reqID].resObject.send({ message: `ticket creation failed` })
      delete requestIds[temp.reqID];
      break;

    default:
      break;
  }

})
consumer.on('error', function (err) {
  console.log('error', err);
});

function productMessage(topic, state, type, reqID, message) {
  return new Promise((resolve, reject) => {
    producer.send([
      {
        topic: topic,
        messages: JSON.stringify({ reqID: reqID, message: message, state: state, type: type })
      }
    ], (err, data) => {
      if (err) {
        console.log(err);
        reject(err)
        // res.json({ message: `massage failed` });

      } else {
        console.log(`${reqID}: current state: ${state}`);

        resolve()
        // res.json({ message: 'message success' });
      }
    });
  })
}