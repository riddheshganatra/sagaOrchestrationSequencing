const kafka = require('kafka-node');

try {
  const Consumer = kafka.Consumer;
  const Producer = kafka.Producer;
  const client = new kafka.KafkaClient(`localhost:2181`);
  const producer = new Producer(client);
  let consumer = new Consumer(
    client,
    [{ topic: `kitchin`, partition: 0 }],
    {
      autoCommit: true,
      fetchMaxWaitMs: 1000,
      fetchMaxBytes: 1024 * 1024,
      encoding: 'utf8',
      fromOffset: false
    }
  );

  consumer.on('message', async function (message) {
    let timeoutTime =100;
    let temp = JSON.parse(message.value);
    if(temp.message == `timeout2`){
      timeoutTime=1000;
    }

    setTimeout(() => {
      switch (temp.state) {
        case 'create-ticket':
          temp.state = `ticket created`;

          if (temp.message == 12) {
            temp.state = `ticket creation failed`;
          }
          producer.send([
            {
              topic: `order-reply-channel`,
              messages: JSON.stringify(temp)
            }
          ], (err, data) => {
            if (err) {
              console.log(err);
            } else {
              console.log(`success`);
            }
          });
          break;

        case 'rollback:create-ticket':
          temp.state = `create-ticket rolledback`;

          producer.send([
            {
              topic: `order-reply-channel`,
              messages: JSON.stringify(temp)
            }
          ], (err, data) => {
            if (err) {
              console.log(err);
            } else {
              console.log(`success`);
            }
          });
          break;

        default:
          break;
      }
      // console.log(message.value);


    }, timeoutTime);

    // consumer.commitOffsets(true);
  })
  consumer.on('error', function (err) {
    console.log('error', err);
  });
}
catch (e) {
  console.log(e);
}