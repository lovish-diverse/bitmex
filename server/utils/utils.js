import _ from "lodash";

export const coins = ["XBTUSD", "ETHUSD"];

const resolution = {
  1: "1min",
  5: "5min",
  15: "15min",
  60: "1h",
  240: "4h",
  1440: "1d",
};

export const getTFfromResolution = (res) => {
  return resolution[res];
};

export const getTimeFromSeconds = (sec) => {
  return new Date(sec * 1000);
};

export const formatOHLCData = (rawData, to, from, intervalInMins) => {
  const intervalInSec = intervalInMins * 60;
  const timeArr = _.range(
    (Math.floor(from / intervalInSec) * (intervalInSec * 1000)) / 1000,
    (Math.ceil(to / intervalInSec) * (intervalInSec * 1000)) / 1000,
    +intervalInSec
  );

  // console.log({ rawData });

  let resObj = { t: timeArr, o: [], h: [], c: [], l: [] };

  rawData.forEach((obj) => {
    // console.log({obj});
    let timeIndex = timeArr.indexOf(obj.openTime.getTime() / 1000);
    // console.log(timeIndex);
    resObj["o"][timeIndex] = obj.open;
    resObj["c"][timeIndex] = obj.close;
    resObj["h"][timeIndex] = obj.high;
    resObj["l"][timeIndex] = obj.low;
  });

  for (let i = 0; i < resObj["o"].length; i++) {
    if (resObj["o"][i] == null || resObj["o"][i] == undefined) {
      resObj["o"][i] = 0;
    }
    if (resObj["c"][i] == null || resObj["c"][i] == undefined) {
      resObj["c"][i] = 0;
    }
    if (resObj["h"][i] == null || resObj["h"][i] == undefined) {
      resObj["h"][i] = 0;
    }
    if (resObj["l"][i] == null || resObj["l"][i] == undefined) {
      resObj["l"][i] = 0;
    }
  }

  return resObj;
};

export const generateRedisKey = ({
  cachePrefix = "trade",
  symbol,
  to,
  from,
  resolution,
}) => {
  return `${cachePrefix}/${symbol}/${from}/${to}/${resolution}`;
};
