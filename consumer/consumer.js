const kafka = require('kafka-node');

try {
  const Consumer = kafka.Consumer;
  const Producer = kafka.Producer;
  const client = new kafka.KafkaClient(`localhost:2181`);
  const producer = new Producer(client);
  let consumer = new Consumer(
    client,
    [{ topic: `verify-consumer`, partition: 0 }],
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
    // console.log(temp);

    setTimeout(() => {

      switch (temp.state) {
        case `verifying consumer`:
          temp.state = `consumer verified`;

          if (temp.message == 1) {
            temp.state = `consumer verification failed`;
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
              // console.log(`success`);
            }
          });
          break;

        case `rollback:verifying consumer`:
              temp.state = `verifying consumer rollbacked`;

              producer.send([
                {
                  topic: `order-reply-channel`,
                  messages: JSON.stringify(temp)
                }
              ], (err, data) => {
                if (err) {
                  console.log(err);
                } else {
                  // console.log(`success`);
                }
              });
          break;

        default:
          break;
      }

    }, 10);

    // consumer.commitOffsets(true);
  })
  consumer.on('error', function (err) {
    console.log('error', err);
  });
}
catch (e) {
  console.log(e);
}