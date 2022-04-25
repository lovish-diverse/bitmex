import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "bitmex-lovish",
  brokers: ["18.217.154.66:9092", "18.217.154.66:9094", "18.217.154.66:9093"],
});

export default kafka;
