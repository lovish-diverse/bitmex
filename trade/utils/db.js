import { MongoClient } from "mongodb";

const url = "mongodb://localhost:27017";
const client = new MongoClient(url);

async function dbConnect(dbName) {
  await client.connect();
  console.log("Connected successfully to server");
  const db = client.db(dbName);
  console.log("Connected successfully to Database:", dbName);
  return db;
}

export default dbConnect;
