const { Kafka } = require('kafkajs');
const stompit = require('stompit');
const async = require('async');

// Configuration for the Kafka brokers
const kafkaConfig = {
    brokers: ["172.30.0.1:9092"],
};

// Create Kafka producer
const kafkaProducer = new Kafka({
    clientId: "rail_app_producer",
    ...kafkaConfig,
}).producer();

// Connect the Kafka producer
const initKafkaProducer = async () => {
    await kafkaProducer.connect();
    console.log('Producer connected successfully');
};

// Initialize Kafka producer
initKafkaProducer();

const connectOptions = {
    host: 'publicdatafeeds.networkrail.co.uk',
    port: 61618,
    connectHeaders: {
        'heart-beat': '15000,15000',
        'client-id': '',
        'host': '/',
        'login': 'vamsiakula196@gmail.com',
        'passcode': 'Adv@37870'
    }
};

const reconnectOptions = {
    initialReconnectDelay: 10,
    maxReconnectDelay: 30000,
    useExponentialBackOff: true,
    maxReconnects: 30,
    randomize: false
};

const connectionManager = new stompit.ConnectFailover([connectOptions], reconnectOptions);

connectionManager.connect((error, client, reconnect) => {
    if (error) {
        console.log('Terminal error, gave up reconnecting');
        return;
    }

    client.on('error', (error) => {
        console.log('Connection lost. Reconnecting...');
        reconnect();
    });

    const headers = {
        destination: '/topic/TRAIN_MVT_ALL_TOC',
        'activemq.subscriptionName': 'somename-train_mvt',
        ack: 'client-individual'
    };

    client.subscribe(headers, (error, message) => {
        if (error) {
            console.log('Subscription failed:', error.message);
            return;
        }

        message.readString('utf-8', (error, body) => {
            if (error) {
                console.log('Failed to read a message', error);
                return;
            }

            if (body) {
                try {
                    const data = JSON.parse(body);

                    async.each(data, (item, next) => {
                        const timestamp = new Date().toISOString();

                        if (item.header && item.header.msg_type === '0003') {
                            // Train Movement
                            const trainMovementData = {
                                TRAIN_ID: item.body.train_id || 'N/A',
                                ACTUAL_TIMESTAMP: item.body.actual_timestamp || 'N/A',
                                LOC_STANOX: item.body.loc_stanox || 'N/A',
                                GBTT_TIMESTAMP: item.body.gbtt_timestamp || 'N/A',
                                PLANNED_TIMESTAMP: item.body.planned_timestamp || 'N/A',
                                PLANNED_EVENT_TYPE: item.body.planned_event_type || 'N/A',
                                EVENT_TYPE: item.body.event_type || 'N/A',
                                EVENT_SOURCE: item.body.event_source || 'N/A',
                                CORRECTION_IND: item.body.correction_ind || 'N/A',
                                OFFROUTE_IND: item.body.offroute_ind || 'N/A',
                                DIRECTION_IND: item.body.direction_ind || 'N/A',
                                LINE_IND: item.body.line_ind || 'N/A',
                                PLATFORM: item.body.platform || 'N/A',
                                ROUTE: item.body.route || 'N/A',
                                TRAIN_SERVICE_CODE: item.body.train_service_code || 'N/A',
                                DIVISION_CODE: item.body.division_code || 'N/A',
                                TOC_ID: item.body.toc_id || 'N/A',
                                TIMETABLE_VARIATION: item.body.timetable_variation || 'N/A',
                                VARIATION_STATUS: item.body.variation_status || 'N/A',
                                NEXT_REPORT_STANOX: item.body.next_report_stanox || 'N/A',
                                NEXT_REPORT_RUN_TIME: item.body.next_report_run_time || 'N/A',
                                TRAIN_TERMINATED: item.body.train_terminated || 'N/A',
                                DELAY_MONITORING_POINT: item.body.delay_monitoring_point || 'N/A',
                                REPORTING_STANOX: item.body.reporting_stanox || 'N/A',
                                AUTO_EXPECTED: item.body.auto_expected || 'N/A',
                            };

                            console.log(timestamp, '- Train Movement recorded:', trainMovementData);

                            // Send the message to Kafka
                            sendToKafka('train_movement', trainMovementData);
                        }

                        next();
                    });
                } catch (e) {
                    console.log('Failed to parse JSON', e);
                }
            }

            client.ack(message);
        });
    });
});

async function sendToKafka(topic, message) {
    try {
        // Use KafkaJS producer to send message to Kafka
        await kafkaProducer.send({
            topic,
            messages: [
                {
                    value: JSON.stringify(message),
                },
            ],
        });

        console.log(`Message sent to Kafka topic "${topic}":`, message);
    } catch (error) {
        console.error('Error sending message to Kafka:', error.message);
    }
}