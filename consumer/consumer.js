const kafka = require('kafka-node');

try {
  
  const Producer = kafka.Producer;
  const client = new kafka.KafkaClient(`localhost:2181`);
  const producer = new Producer(client);

  var options = {
    // connect directly to kafka broker (instantiates a KafkaClient)
    kafkaHost: 'localhost:9092',
    groupId: 'testGroup',
    autoCommit: true,
    autoCommitIntervalMs: 5000,
    sessionTimeout: 15000,
    fetchMaxBytes: 10 * 1024 * 1024, // 10 MB
    // An array of partition assignment protocols ordered by preference. 'roundrobin' or 'range' string for
    // built ins (see below to pass in custom assignment protocol)
    protocol: ['roundrobin'],
    // Offsets to use for new groups other options could be 'earliest' or 'none'
    // (none will emit an error if no offsets were saved) equivalent to Java client's auto.offset.reset
    fromOffset: 'latest',
    // how to recover from OutOfRangeOffset error (where save offset is past server retention)
    // accepts same value as fromOffset
    outOfRangeOffset: 'earliest',
    onRebalance: (isAlreadyMember, callback) => { callback(); }
  };

  var consumerGroup = new kafka.ConsumerGroup(options, 'consumer');
  // let consumer = new Consumer(
  //   client,
  //   [{ topic: `consumer`, partition: 0 }],
  //   {
  //     autoCommit: true,
  //     fetchMaxWaitMs: 1000,
  //     fetchMaxBytes: 1024 * 1024,
  //     encoding: 'utf8',
  //     fromOffset: false,
  //     groupId: 'ExampleTestGroup'
  //   }
  // );

  consumerGroup.on('ready',()=>{
    console.log(`ready`);
    
  })
  
  consumerGroup.on('message', async function (message) {
    console.log(`message`);
    
    let temp = JSON.parse(message.value);
    // console.log(temp);
    let timeoutTime=100;
    if(temp.message==`timeout1`){
      timeoutTime = 1000;
    }
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

    }, timeoutTime);

    // consumer.commitOffsets(true);
  })
  consumerGroup.on('error', function (err) {
    console.log('error', err);
  });
}
catch (e) {
  console.log(e);
}