const requestPromise = require('request-promise');
var faker = require('faker')
async function start(){
    let promises=[]
for (let index = 0; index < 50; index++) {
    promises.push(requestPromise({
        method: 'GET',
        uri: `http://localhost:8080/api?message=${faker.name.findName()}`,
        json: true
    }))
    
}
console.log(await Promise.all(promises));
 
}
start();
 