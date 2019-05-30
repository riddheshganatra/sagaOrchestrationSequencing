const requestPromise = require('request-promise');
async function start(){
    let promises=[]
for (let index = 0; index < 20; index++) {
    promises.push(requestPromise({
        method: 'GET',
        uri: `http://localhost:8080/api?message=123`,
        json: true
    }))
    
}
console.log(await Promise.all(promises));
 
}
start();
 