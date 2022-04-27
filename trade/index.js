import WebSocket from "ws";
import kafka from "./kafka.js";
import dbConnect from "./utils/db.js";
import crypto from "crypto";

import { getOpenTime, getCloseTime, getDefaultData } from "./utils/utils.js";

class Trade {
  constructor(coin) {
    this.coin = coin;
    this.socketURL = `wss://ws.bitmex.com/realtime?subscribe=trade:${coin}`;
    this.ws = new WebSocket(this.socketURL);
    this.oneMinRecord = getDefaultData();
    this.fiveMinRecord = getDefaultData();
    this.fifteenMinRecord = getDefaultData();
    this.oneHourRecord = getDefaultData();
    this.fourHourRecord = getDefaultData();
    this.oneDayRecord = getDefaultData();
    this.db = dbConnect(this.coin);
  }

  getData() {
    const d = new Date().getTime();
    let oneMinOpenTime = getOpenTime(d, 1);
    let oneMinCloseTime = getCloseTime(d, 1);
    let fiveMinOpenTime = getOpenTime(d, 5);
    let fiveMinCloseTime = getCloseTime(d, 5);
    let fifteenMinOpenTime = getOpenTime(d, 15);
    let fifteenMinCloseTime = getCloseTime(d, 15);
    let oneHourOpenTime = getOpenTime(d, 60);
    let oneHourCloseTime = getCloseTime(d, 60);
    let fourHourOpenTime = getOpenTime(d, 4 * 60);
    let fourHourCloseTime = getCloseTime(d, 4 * 60);
    let oneDayOpenTime = getOpenTime(d, 24 * 60);
    let oneDayCloseTime = getCloseTime(d, 24 * 60);

    this.ws.on("message", async (data) => {
      const reqData = JSON.parse(data.toString());
      // console.log(reqData);
      if (typeof reqData !== undefined && reqData.data) {
        const timeStamp = new Date(reqData?.data?.[0]?.timestamp);

        // console.log("timestamp", timeStamp);
        // console.log("start", oneMinOpenTime);
        // console.log("end", oneMinCloseTime);
        // console.log(timeStamp < oneMinCloseTime);

        if (timeStamp <= oneMinCloseTime) {
          console.log("UNDER 1 MIN");
          const recordArrInSameTime = reqData?.data;
          this.oneMinRecord = this.updateOHCLValues(
            recordArrInSameTime,
            this.oneMinRecord
          );
        } else {
          this.oneMinRecord.openTime = oneMinOpenTime;
          this.oneMinRecord.closeTime = oneMinCloseTime;
          this.oneMinRecord.key = "1min";
          await this.pushToDB(this.oneMinRecord);
          this.oneMinRecord.open = "";
          oneMinOpenTime = oneMinCloseTime;
          oneMinCloseTime = getCloseTime(timeStamp.getTime(), 1);
          const recordArrInSameTime = reqData?.data;
          this.oneMinRecord = this.updateOHCLValues(
            recordArrInSameTime,
            this.oneMinRecord
          );
        }

        if (timeStamp <= fiveMinCloseTime) {
          console.log("UNDER 5 MIN");
          const recordArrInSameTime = reqData?.data;
          this.fiveMinRecord = this.updateOHCLValues(
            recordArrInSameTime,
            this.fiveMinRecord
          );
        } else {
          this.fiveMinRecord.openTime = fiveMinOpenTime;
          this.fiveMinRecord.closeTime = fiveMinCloseTime;
          this.fiveMinRecord.key = "5min";
          await this.pushToDB(this.fiveMinRecord);
          this.fiveMinRecord.open = "";
          fiveMinOpenTime = fiveMinCloseTime;
          fiveMinCloseTime = getCloseTime(timeStamp.getTime(), 5);
          const recordArrInSameTime = reqData?.data;
          this.fiveMinRecord = this.updateOHCLValues(
            recordArrInSameTime,
            this.fiveMinRecord
          );
        }

        if (timeStamp <= fifteenMinCloseTime) {
          console.log("UNDER 15 MIN");
          const recordArrInSameTime = reqData?.data;
          this.fifteenMinRecord = this.updateOHCLValues(
            recordArrInSameTime,
            this.fifteenMinRecord
          );
        } else {
          this.fifteenMinRecord.openTime = fifteenMinOpenTime;
          this.fifteenMinRecord.closeTime = fifteenMinCloseTime;
          this.fifteenMinRecord.key = "15min";
          await this.pushToDB(this.fifteenMinRecord);
          this.fifteenMinRecord.open = "";
          fifteenMinOpenTime = fifteenMinCloseTime;
          fifteenMinCloseTime = getCloseTime(timeStamp.getTime(), 15);
          const recordArrInSameTime = reqData?.data;
          this.fifteenMinRecord = this.updateOHCLValues(
            recordArrInSameTime,
            this.fifteenMinRecord
          );
        }

        if (timeStamp <= oneHourCloseTime) {
          console.log("UNDER 1 HOUR");
          const recordArrInSameTime = reqData?.data;
          this.oneHourRecord = this.updateOHCLValues(
            recordArrInSameTime,
            this.oneHourRecord
          );
        } else {
          this.oneHourRecord.openTime = oneHourOpenTime;
          this.oneHourRecord.closeTime = oneHourCloseTime;
          this.oneHourRecord.key = "1h";
          await this.pushToDB(this.oneHourRecord);
          this.oneHourRecord.open = "";
          oneHourOpenTime = oneHourCloseTime;
          oneHourCloseTime = getCloseTime(timeStamp.getTime(), 60);
          const recordArrInSameTime = reqData?.data;
          this.oneHourRecord = this.updateOHCLValues(
            recordArrInSameTime,
            this.oneHourRecord
          );
        }

        if (timeStamp <= fourHourCloseTime) {
          console.log("UNDER  4 HOUR");
          const recordArrInSameTime = reqData?.data;
          this.fourHourRecord = this.updateOHCLValues(
            recordArrInSameTime,
            this.fourHourRecord
          );
        } else {
          this.fourHourRecord.openTime = fourHourOpenTime;
          this.fourHourRecord.closeTime = fourHourCloseTime;
          this.fourHourRecord.key = "4h";
          await this.pushToDB(this.fourHourRecord);
          this.fourHourRecord.open = "";
          fourHourOpenTime = fourHourCloseTime;
          fourHourCloseTime = getCloseTime(timeStamp.getTime(), 4 * 60);
          const recordArrInSameTime = reqData?.data;
          this.fourHourRecord = this.updateOHCLValues(
            recordArrInSameTime,
            this.fourHourRecord
          );
        }

        if (timeStamp <= oneDayCloseTime) {
          console.log("UNDER 1 DAY");
          const recordArrInSameTime = reqData?.data;
          this.oneDayRecord = this.updateOHCLValues(
            recordArrInSameTime,
            this.oneDayRecord
          );
        } else {
          this.oneDayRecord.openTime = oneDayOpenTime;
          this.oneDayRecord.closeTime = oneDayCloseTime;
          this.oneDayRecord.key = "1d";
          await this.pushToDB(this.oneDayRecord);
          this.oneDayRecord.open = "";
          oneDayOpenTime = oneDayCloseTime;
          oneDayCloseTime = getCloseTime(timeStamp.getTime(), 24 * 60);
          const recordArrInSameTime = reqData?.data;
          this.oneDayRecord = this.updateOHCLValues(
            recordArrInSameTime,
            this.oneDayRecord
          );
        }
      }
    });
  }

  async pushToDB(data) {
    try {
      const doc = await (await this.db)
        .collection(`${this.coin}-Trade-${data.key}`)
        .insertOne({ ...data, createdAt: new Date() });
      if (doc) {
        console.log(
          `Document added successfully for ${data.type} ${data.key}`,
          doc
        );
      }
    } catch (error) {
      console.error(error);
    }
  }

  async pushToKafka() {
    const producer = kafka.producer({ allowAutoTopicCreation: true });
    await producer.connect();
    console.log(`Connected to lovish-${this.coin} topic`);
    setInterval(() => {
      producer.send({
        topic: `lovish-${this.coin}`,
        messages: [
          {
            key: crypto.randomUUID(),
            value: JSON.stringify({
              "1min": this.oneMinRecord,
              "5min": this.fiveMinRecord,
              "15min": this.fifteenMinRecord,
              "1h": this.oneHourRecord,
              "4h": this.fourHourRecord,
              "1d": this.oneDayRecord,
              type: this.coin,
            }),
          },
        ],
      });
    }, 1000);
  }

  updateOHCLValues(dataArr, initialValues) {
    // console.log({ initialValues });
    dataArr.forEach((elem) => {
      if (!initialValues.open) {
        initialValues = {
          open: elem.price,
          close: elem.price,
          high: elem.price,
          low: elem.price,
          type: this.coin,
        };
      }
      if (initialValues.high < elem.price) {
        initialValues["high"] = elem.price;
      }
      if (initialValues.low > elem.price) {
        initialValues["low"] = elem.price;
      }
      initialValues["close"] = elem.price;
    });
    return initialValues;
  }
}

const t = new Trade("XBTUSD");
t.getData();
t.pushToKafka();

const t2 = new Trade("ETHUSD");
t2.getData();
t2.pushToKafka();
