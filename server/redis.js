import { createClient } from "redis";
import { generateRedisKey } from "./utils/utils.js";

export const redisClient = createClient();
redisClient.on("error", (err) => console.log("Redis Client Error", err));

export const cacheMiddleware = async (req, res, next) => {
  await redisClient.connect();

  const cachedData = await redisClient.get(generateRedisKey(req.query));

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
