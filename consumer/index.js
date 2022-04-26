import kafka from "./kafka.js";
import WebSocket, { WebSocketServer } from "ws";
import http from "http";

const PORT = 8080;
const topics = ["lovish-XBTUSD", "lovish-ETHUSD"];
const consumer = kafka.consumer({ groupId: "lovish-consumer-group" });

const server = http.createServer(function (request, response) {
  response.writeHead(200, { "Content-Type": "text/html" });
  response.write("message ==> Server is live.");
  response.end();
});

const wss = new WebSocketServer({
  server,
});

wss.on("connection", function connection(ws, req) {
  console.log("Connected to websocket server");

  ws.requestedCoin = req.url.split("/")[1];
  console.log(req.url.split("/"));
  ws["coin"] = req.url.split("/")[1];
  ws["tf"] = req.url.split("/")[2];

  ws.on("message", (req) => {
    ws.send("Working");
  });

  ws.on("close", (req) => {
    console.log("Websocket client connection closed", req);
  });
});

const run = async () => {
  await consumer.connect();
  topics.forEach(async (t) => {
    if (t) {
      await consumer.subscribe({
        topic: t,
        fromBeginning: false,
      });
    }
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;

      console.log(
        "======================================================================="
      );

      console.log(
        `- ${prefix} ${message.key}# `,
        JSON.parse(message.value.toString())
      );

      console.log(
        "======================================================================="
      );

      wss.clients.forEach(function each(client) {
        let type = JSON.parse(message.value.toString()).type;
        // console.log(client["coin"]);
        // console.log(type);

        if (client["coin"] === type) {
          console.log(JSON.parse(message.value.toString())[client["tf"]]);
          client.send(
            JSON.stringify(JSON.parse(message.value.toString())[client["tf"]])
          );
        }
      });
    },
  });
};

run().catch((e) => console.error(`${e.message}`, e));

wss.on("close", (req) =>
  console.log("WebSocket server Connection Closed", req)
);

server.listen(PORT, () => {
  console.log(`Server is up and running on port ${PORT}`);
});
