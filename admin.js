const { Kafka } = require("kafkajs");

exports.kafka = new Kafka({
  clientId: "my-app",
  brokers: ["172.30.0.1:9092"],
});

async function init() {
  const admin = exports.kafka.admin();
  console.log("Admin connecting...");
  await admin.connect();
  console.log("Admin Connection Success...");

  console.log("Creating Topics [train_movement, weather_data]");
  await admin.createTopics({
    topics: [
      { topic: "train_movement", numPartitions: 2 },
      { topic: "weather_data", numPartitions: 2 },
    ],
  });
  console.log("Topics Created Success [train_movement, weather_data]");

  console.log("Disconnecting Admin...");
  await admin.disconnect();
}

init();