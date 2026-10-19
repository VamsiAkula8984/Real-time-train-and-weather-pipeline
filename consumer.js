const { Kafka, logLevel } = require('kafkajs');
const snowflake = require('snowflake-sdk');

// Create Snowflake connection
const snowflakeConnection = snowflake.createConnection({
  account: 'WISMGCK-TW41160',
  username: 'HarryPotter',
  password: 'Advvamsi@37870',
  warehouse: 'rail_weather',
  database: 'rail_data',
  schema: 'public'
});

// Connect to Snowflake
snowflakeConnection.connect((err, conn) => {
  if (err) {
    console.error('Unable to connect to Snowflake:', err.message);
  } else {
    console.log('Connected to Snowflake.');
  }
});

// Configuration for the Kafka brokers
const kafkaConfig = {
  brokers: ['172.30.0.1:9092'],
  logLevel: logLevel.DEBUG, // Set log level to DEBUG for detailed logging
};

// Create Kafka consumer
const kafka = new Kafka({
  clientId: 'rail_app_consumer',
  ...kafkaConfig,
});
const kafkaConsumer = kafka.consumer({
  groupId: 'rail_consumer_group',
});

// Topics produced by the producer
const topics = ['train_movement'];

// Connect the Kafka consumer
const initKafkaConsumer = async () => {
  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe({ topics });

  await kafkaConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const processedMessage = JSON.parse(message.value.toString('utf-8'));

        // Log the received message
        console.log(processedMessage);

        // Insert data into Snowflake database
        if (topic === 'train_movement') {
          insertTrainMovement(processedMessage);
        }

        // Add your processing logic here
        // For now, let's log that the message is being processed
        console.log('Processing message...');
      } catch (error) {
        console.error('Error processing message:', error.message);
      }
    },
  });
};

// Function to insert train movement data into the database
function insertTrainMovement(data) {
  const sql = `
    INSERT INTO TRAIN_MOVEMENT (
      TRAIN_ID, ACTUAL_TIMESTAMP, LOC_STANOX, GBTT_TIMESTAMP, PLANNED_TIMESTAMP, PLANNED_EVENT_TYPE, 
      EVENT_TYPE, EVENT_SOURCE, CORRECTION_IND, OFFROUTE_IND, DIRECTION_IND, LINE_IND, PLATFORM, ROUTE, 
      TRAIN_SERVICE_CODE, DIVISION_CODE, TOC_ID, TIMETABLE_VARIATION, VARIATION_STATUS, NEXT_REPORT_STANOX, 
      NEXT_REPORT_RUN_TIME, TRAIN_TERMINATED, DELAY_MONITORING_POINT, REPORTING_STANOX, AUTO_EXPECTED
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;
  const values = [
    data.TRAIN_ID,
    data.ACTUAL_TIMESTAMP,
    data.LOC_STANOX,
    data.GBTT_TIMESTAMP,
    data.PLANNED_TIMESTAMP,
    data.PLANNED_EVENT_TYPE,
    data.EVENT_TYPE,
    data.EVENT_SOURCE,
    data.CORRECTION_IND,
    data.OFFROUTE_IND,
    data.DIRECTION_IND,
    data.LINE_IND,
    data.PLATFORM,
    data.ROUTE,
    data.TRAIN_SERVICE_CODE,
    data.DIVISION_CODE,
    data.TOC_ID,
    data.TIMETABLE_VARIATION,
    data.VARIATION_STATUS,
    data.NEXT_REPORT_STANOX,
    data.NEXT_REPORT_RUN_TIME,
    data.TRAIN_TERMINATED,
    data.DELAY_MONITORING_POINT,
    data.REPORTING_STANOX,
    data.AUTO_EXPECTED
  ];

  snowflakeConnection.execute({
    sqlText: sql,
    binds: values,
    complete: (err, stmt, rows) => {
      if (err) {
        console.error('Error inserting train movement data:', err.message);
      } else {
        console.log('Inserted train movement data:', rows);
      }
    }
  });
}

// Initialize Kafka consumer
initKafkaConsumer().catch(error => {
  console.error('Error initializing Kafka consumer:', error);
});

// Handle process termination to close the Kafka consumer gracefully
process.on('SIGTERM', async () => {
  await kafkaConsumer.disconnect();
  process.exit(0);
});

process.on('SIGINT', async () => {
  await kafkaConsumer.disconnect();
  process.exit(0);
});