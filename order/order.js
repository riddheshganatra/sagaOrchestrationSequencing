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
      resObject: res,
      timeoutFunction: setTimeout(function () {
        delete requestIds[reqID];
        res.send(`request timeout`)
      }, 500)
    }

    // verify-consumer
    await productMessage(`verify-consumer`, `verifying consumer`, reqID, req.query.message);

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
      //check for timeout
      if (requestIds[temp.reqID] == undefined) {

        return await productMessage(`verify-consumer`, `rollback:verifying consumer`, temp.reqID, temp.message);

      }

        // update state
        await productMessage(`create-ticket`, `create-ticket`, temp.reqID, temp.message);
      
      break;

    case `consumer verification failed`:
      requestIds[temp.reqID].resObject.send({ message: 'consumer verification failed' })
      break;

    case `ticket created`:
        if (requestIds[temp.reqID] == undefined) {
          return productMessage(`create-ticket`, `rollback:create-ticket`, temp.reqID, temp.message);
        }
      // send response back
      requestIds[temp.reqID].resObject.send({ message: temp.message })
      clearTimeout(requestIds[temp.reqID].timeoutFunction);
      delete requestIds[temp.reqID];
      break;

    case `ticket creation failed`:

      await productMessage(`verify-consumer`, `rollback:verifying consumer`, temp.reqID, temp.message);

      break;

    case `verifying consumer rollbacked`:
        if (requestIds[temp.reqID] == undefined) {
          return;
        }

      requestIds[temp.reqID].resObject.send({ message: `ticket creation failed` })
      clearTimeout(requestIds[temp.reqID].timeoutFunction);
      delete requestIds[temp.reqID];
      break;

    case `create-ticket rolledback`:
      await productMessage(`verify-consumer`, `rollback:verifying consumer`, temp.reqID, temp.message);
        
      break;

    default:
      break;
  }

})
consumer.on('error', function (err) {
  console.log('error', err);
});

function productMessage(topic, state, reqID, message) {
  return new Promise((resolve, reject) => {
    producer.send([
      {
        topic: topic,
        messages: JSON.stringify({ reqID: reqID, message: message, state: state })
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