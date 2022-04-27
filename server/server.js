import express from "express";
import { client, dbConnect } from "./db/db.js";
import {
  coins,
  formatOHLCData,
  getTFfromResolution,
  getTimeFromSeconds,
} from "./utils/utils.js";
import createEncryptor from "simple-encryptor";

const app = express();
const PORT = process.env.PORT || 5000;
const SECRET = process.env.SECRET || "HiIamASecretKeyThatYouCanNotGuess";
const encryptor = createEncryptor(SECRET);

app.get("/ping", (req, res) => {
  res.send("Server is live!! Pong!!!");
});

app.get("/api/trade", async (req, res) => {
  const timeFrame = getTFfromResolution(req.query.resolution);
  const coin = `${req.query.symbol}USD`;
  const toTime = getTimeFromSeconds(req.query.to);
  const fromTime = getTimeFromSeconds(req.query.from);

  // console.log({ toTime, fromTime });

  // Checking here if we serve this coin stats or not.
  if (!coins.includes(coin)) {
    res.status(404).json({
      success: false,
      message: "No Coin With this name",
    });
  } else {
    dbConnect(coin)
      .then(async (db) => {
        console.log(`Getting data for: ${coin} for time frame: ${timeFrame}`);
        const records = await db
          .collection(`${coin}-Trade-${timeFrame}`)
          .aggregate([
            {
              $match: {
                $expr: {
                  $or: [
                    { $gte: ["$openTime", fromTime] },
                    { $lte: ["$closeTime", toTime] },
                  ],
                },
              },
            },
          ])
          .toArray();

        res.status(200).json({
          success: true,
          message: "Success",
          data: encryptor.encrypt(
            formatOHLCData(
              records,
              req.query.to,
              req.query.from,
              req.query.resolution
            )
          ),
        });

        client.close(() => {
          console.log("Connection closed successfully");
        });
      })
      .catch((err) => console.log("Something went wrong!!", err.message));
  }
});

app.listen(PORT, (req, res) => {
  console.log(`Server up and running on port ${PORT}`);
});
