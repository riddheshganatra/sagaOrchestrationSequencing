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

  var consumerGroup = new kafka.ConsumerGroup(options, 'kitchin');
  consumerGroup.on('ready',()=>{
    console.log(`ready`);
    
  })

  consumerGroup.on('message', async function (message) {
    let timeoutTime =100;
    let temp = JSON.parse(message.value);

    
    if(temp.message == `timeout2`){
      timeoutTime=2000;
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
  consumerGroup.on('error', function (err) {
    console.log('error', err);
  });
}
catch (e) {
  console.log(e);
}