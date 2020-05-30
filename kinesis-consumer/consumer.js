var deagg = require('aws-kinesis-agg');
var async = require('async');
var AWS = require('aws-sdk');
require('constants');
var computeChecksums = true;

var ok = 'OK';
var error = 'ERROR';
// get reference to S3 client
var s3 = new AWS.S3();

/** function which closes the context correctly based on status and message */
var finish = function(event, context, status, message) {
	"use strict";

	console.log("Processing Complete");

	// log the event if we've failed
	if (status !== ok) {
		if (message) {
			console.log(message);
		}

		// ensure that Lambda doesn't checkpoint to kinesis
		context.done(status, JSON.stringify(message));
	} else {
		context.done(null, message);
	}
};

/** function which handles cases where the input message is malformed */
var handleNoProcess = function(event, callback) {
	"use strict";

	var noProcessReason;

	if (!event.Records || event.Records.length === 0) {
		noProcessReason = "Event contains no Data";
	}
	if (event.Records[0].eventSource !== "aws:kinesis") {
		noProcessReason = "Invalid Event Source " + event.eventSource;
	}
	if (event.Records[0].kinesis.kinesisSchemaVersion !== "1.0") {
		noProcessReason = "Unsupported Event Schema Version " + event.Records[0].kinesis.kinesisSchemaVersion;
	}

	if (noProcessReason) {
		finish(event, error, noProcessReason);
	} else {
		callback();
	}
};

/**
 * Example lambda function which uses the KPL syncronous deaggregation interface
 * to process Kinesis Records from the Event Source
 */
module.exports.kinesis_deaggregator_handler = function(event, context) {
	"use strict";

	console.log("Processing KPL Aggregated Messages using kpl-deagg(sync)");

	handleNoProcess(event, function() {
		console.log("Processing " + event.Records.length + " Kinesis Input Records");
		var totalUserRecords = 0;

		async.map(event.Records, function(record, asyncCallback) {
			// use the deaggregateSync interface which receives a single
			// callback with an error and an array of Kinesis Records
			deagg.deaggregateSync(record.kinesis, computeChecksums, function(err, userRecords) {
				if (err) {
					console.log(err);
					asyncCallback(err);
				} else {
					console.log("Received " + userRecords.length + " Kinesis User Records");
					totalUserRecords += userRecords.length;

					userRecords.map(function(record) {
						var recordData = new Buffer(record.data, 'base64');

						console.log("Kinesis Aggregated User Record:" + recordData.toString('ascii'));

						var params = {
							Body: "This is test harish",
							Bucket: "kinesis-consumer-dev-us-east-1-eventdata",
							Key: "tenant/Netwitness/NetworkTraffic/region/sessiondata.json"
						};

						s3.putObject(params, function(err, data) {
							if (err) console.log(err, err.stack); // an error occurred
							else     console.log(data);           // successful response
						});

						// you can do something else with each kinesis
						// user record here!
					});

					// call the async callback to reflect that the kinesis
					// message is completed
					asyncCallback(err);
				}
			});
		}, function(err, results) {
			// function is called once all kinesis records have been processed
			// by async.map


			if (err) {
				finish(event, context, error, err);
			} else {
				finish(event, context, ok, "Success");
			}
		});
	});
};
