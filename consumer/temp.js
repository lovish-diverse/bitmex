import express from "express";
var server = express();
var connectionsIDS = new Set();
var user = {};
const key = "hyblock_#2020$forCrypto";
var encryptor = require("simple-encryptor")(key);
import { Kafka } from "kafkajs";
const kafka = new Kafka({
  clientId: "bitmex-trollbox",
  brokers: ["10.0.0.5:9092", "10.0.0.5:9094", "10.0.0.5:9093"],
});
const consumer = kafka.consumer({ groupId: "TROLLBOX_USERS" });
/*@dataHold Variable for bitmex XBT start*/
var TRBX_USERS_XBT = {
  1: {},
  5: {},
  15: {},
  "1hr": {},
  "4hr": {},
  "1day": {},
};
var TRBX_USERS_ETH = {
  1: {},
  5: {},
  15: {},
  "1hr": {},
  "4hr": {},
  "1day": {},
};

////Global OpenInterest end ////
import { createServer } from "http";
var server = createServer(function (request, response) {
  response.writeHead(200, { "Content-Type": "text/html" });
  response.write("message ==>server is live.");
  response.end();
});
import { Server as WebSocketServer } from "ws";
var wss = new WebSocketServer({ server });
// Consuming
consumer.connect();
consumer.subscribe({ topic: "bitmex_trollbox_xbt", fromBeginning: false });
consumer.subscribe({ topic: "bitmex_trollbox_eth", fromBeginning: false });

consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log("UNLEEEEEE_____", topic);
    if (topic == "bitmex_trollbox_xbt") {
      TRBX_USERS_XBT["1"] = JSON.parse(message.value)["1"];
      TRBX_USERS_XBT["5"] = JSON.parse(message.value)["5"];
      TRBX_USERS_XBT["15"] = JSON.parse(message.value)["15"];
      TRBX_USERS_XBT["1hr"] = JSON.parse(message.value)["1hr"];
      TRBX_USERS_XBT["4hr"] = JSON.parse(message.value)["4hr"];
      TRBX_USERS_XBT["1day"] = JSON.parse(message.value)["1day"];

      console.log(JSON.parse(message.value));
    } else if (topic == "bitmex_trollbox_eth") {
      TRBX_USERS_ETH["1"] = JSON.parse(message.value)["1"];
      TRBX_USERS_ETH["5"] = JSON.parse(message.value)["5"];
      TRBX_USERS_ETH["15"] = JSON.parse(message.value)["15"];
      TRBX_USERS_ETH["1hr"] = JSON.parse(message.value)["1hr"];
      TRBX_USERS_ETH["4hr"] = JSON.parse(message.value)["4hr"];
      TRBX_USERS_ETH["1day"] = JSON.parse(message.value)["1day"];

      console.log(JSON.parse(message.value));
    }
    wss.clients.forEach(function each(client) {
      if (client.resolution == "1" && client.symbol == "xbt") {
        console.log("Uncle---->>>", getValueXBT(client.subscribe, "1"));
        client.send(getValueXBT(client.subscribe, "1"));
      }
      if (client.resolution == "5" && client.symbol == "xbt") {
        client.send(getValueXBT(client.subscribe, "5"));
      }
      if (client.resolution == "15" && client.symbol == "xbt") {
        client.send(getValueXBT(client.subscribe, "15"));
      }
      if (client.resolution == "1hr" && client.symbol == "xbt") {
        client.send(getValueXBT(client.subscribe, "1hr"));
      }
      if (client.resolution == "4hr" && client.symbol == "xbt") {
        client.send(getValueXBT(client.subscribe, "4hr"));
      }
      if (client.resolution == "1day" && client.symbol == "xbt") {
        client.send(getValueXBT(client.subscribe, "1day"));
      }
      if (client.resolution == "1" && client.symbol == "eth") {
        client.send(getValueETH(client.subscribe, "1"));
      }
      if (client.resolution == "5" && client.symbol == "eth") {
        client.send(getValueETH(client.subscribe, "5"));
      }
      if (client.resolution == "15" && client.symbol == "eth") {
        client.send(getValueETH(client.subscribe, "15"));
      }
      if (client.resolution == "1hr" && client.symbol == "eth") {
        client.send(getValueETH(client.subscribe, "1hr"));
      }
      if (client.resolution == "4hr" && client.symbol == "eth") {
        client.send(getValueETH(client.subscribe, "4hr"));
      }
      if (client.resolution == "1day" && client.symbol == "eth") {
        client.send(getValueETH(client.subscribe, "1day"));
      }
    });
  },
});
function getValueXBT(Indicator, resolution) {
  console.log("Indicator", Indicator);
  switch (Indicator) {
    case "trboxhandler":
      console.log("Inside CASE !!!!!");
      let re = encryptor.encrypt({
        ...TRBX_USERS_XBT[resolution],
      });
      console.log(re);
      return re;

    // return encryptor.encrypt({
    //     bitmex: BITMEX_BTC[resolution],
    //     binance: BINANCE_BTC[resolution],
    //     huobi: HUOBI_BTC[resolution],
    //     deribit: DERIBIT_BTC[resolution],
    //     okex: OKEX_BTC[resolution],
    //     okexQTRLY: OKEX_QTRLY_BTC[resolution],
    // })
    default:
      console.log("1st DEfaulty---");
      return encryptor.encrypt({});
  }
}
function getValueETH(Indicator, resolution) {
  ///// console.log("Indicator",Indicator)
  switch (Indicator) {
    case "trboxhandler":
      return encryptor.encrypt({
        ...TRBX_USERS_ETH[resolution],
      });

    // return encryptor.encrypt({
    //     bitmex: BITMEX_BTC[resolution],
    //     binance: BINANCE_BTC[resolution],
    //     huobi: HUOBI_BTC[resolution],
    //     deribit: DERIBIT_BTC[resolution],
    //     okex: OKEX_BTC[resolution],
    //     okexQTRLY: OKEX_QTRLY_BTC[resolution],
    // })
    default:
      return encryptor.encrypt({});
  }
}

wss.on("connection", function (ws, req) {
  let _url = req.url.split("/");
  console.log("==================", _url);
  let _url_user = _url[_url.length - 1];
  let resolutin = _url[_url.length - 2];
  let symbol = _url[_url.length - 3];
  ws["subscribe"] = _url_user;
  ws["symbol"] = symbol;
  ws["resolution"] = resolutin;
  ws.on("message", function (message) {
    let ID = JSON.parse(message).email;
    if (JSON.parse(message)["subscribe"] != undefined) {
      user[ID]["subscribe"] = JSON.parse(message)["subscribe"];
    }
  });
  ws.on("close", (req) => {
    console.log("close", req);
  });
});
wss.on("close", (req) => {
  console.log("close", req);
});
server.listen(8010, () => {
  console.log(`8010 listening on port `);
});
