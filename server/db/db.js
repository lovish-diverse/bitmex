import { MongoClient } from "mongodb";

const url = "mongodb://localhost:27017";
export const client = new MongoClient(url);

export async function dbConnect(database) {
  await client.connect();
  console.log("Connected successfully to server");
  const db = client.db(database);
  console.log("Connected successfully to Database:", database);
  return db;
}
