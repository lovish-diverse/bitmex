import { createClient } from "redis";
import { generateRedisKey } from "./utils/utils.js";

export const redisClient = createClient();

export const cacheMiddleware = async (req, res, next) => {
  redisClient.on("error", (err) => console.log("Redis Client Error", err));

  await redisClient.connect();

  const cachedData = await redisClient.get(generateRedisKey(req.query));

  console.log(generateRedisKey(req.query));

  if (cachedData) {
    await redisClient.disconnect();
    res.status(200).json({
      success: true,
      message: "Success",
      data: cachedData,
    });
  } else {
    next();
  }
};
