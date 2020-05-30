const AWS = require('aws-sdk');

AWS.config.update({ region: 'us-east-1' });

var credentials = new AWS.SharedIniFileCredentials({profile: 'harish.sg2020'});
AWS.config.credentials = credentials;

const kinesis = new AWS.Kinesis();

setInterval( async () => {
    try {
        let data = await generateNotesItem();
        let params = {
            Data: new Buffer(JSON.stringify(data)),
            PartitionKey: 'P1',
            StreamName: 'KinesislambdaStream'
        };
        let response = await kinesis.putRecord(params).promise();
        console.log("Data:", data);
        console.log("Response:", response);
        console.log();
    } catch(err) {
        throw err;
    }
}, 1000);

generateNotesItem = async () => {
    return ({
        "Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "1",
                "sequenceNumber": "49590338271490256608559692538361571095921575989136588898",
                "data": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0Lg==",
                "approximateArrivalTimestamp": 1545084650.987
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000006:49590338271490256608559692538361571095921575989136588898",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::123456789012:role/lambda-role",
            "awsRegion": "us-east-2",
            "eventSourceARN": "arn:aws:kinesis:us-east-2:123456789012:stream/lambda-stream"
        }
    ]
    });
}
