const mongoose = require("mongoose");
const redis = require("redis");
const util = require("util");
const keys = require("../config/keys");
const client = redis.createClient({
  host: keys.redisHost,
  port: keys.redisPort,
  retry_strategy: () => 1000
});
client.get = util.promisify(client.get);
const exec = mongoose.Query.prototype.exec;
mongoose.Query.prototype.exec = async function() {
  const key = JSON.stringify({
    ...this.getQuery()
  });
 
  const cacheValue = await client.get(key);
  if (cacheValue) {
    const doc = JSON.parse(cacheValue);
    console.log("Response from Redis");
    return Array.isArray(doc)
      ? doc.map(d => new this.model(d))
      : new this.model(doc);
  }
  const result = await exec.apply(this, arguments);
  client.set(key, JSON.stringify(result));
  console.log("Response from MongoDB");
  return result;
};
