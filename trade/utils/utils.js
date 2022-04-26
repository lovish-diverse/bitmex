const msIn1min = 60000;

const getOpenTime = (time, interval) => {
  return new Date(
    Math.floor(time / (interval * msIn1min)) * (interval * msIn1min)
  );
};

const getCloseTime = (time, interval) => {
  return new Date(
    Math.ceil(time / (interval * msIn1min)) * (interval * msIn1min)
  );
};

const getDefaultData = () => ({
  high: "",
  low: "",
  open: "",
  close: "",
  openTime: "",
  closeTime: "",
  key: "",
});

export { getCloseTime, getOpenTime, getDefaultData };
