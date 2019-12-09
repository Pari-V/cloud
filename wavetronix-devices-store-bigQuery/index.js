/**
 * Triggered from a change to a Cloud Storage bucket.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
'use strict';

const Storage = require('@google-cloud/storage');
const BigQuery = require('@google-cloud/bigquery');

// Instantiates a client
const storage = Storage();
const bigquery = new BigQuery();

const datasetId = "wavetronix_device_info";
const tableId = "devices";


exports.moveDevicesFileFromGCSToDeviceTable = (data, context) => {
    let job;
    const file = data;
    const filename=file.name;
    if (filename.startsWith("device")){
        console.log(`Metadata of event and Job: Event ${context.eventId}; Event Type: ${context.eventType}; Bucket: ${file.bucket}; File: ${file.name}; Metageneration: ${file.metageneration}; Created: ${file.timeCreated}; Updated: ${file.updated};`);
        
        const bucketname = file.bucket;

        const jobMetadata = {
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            writeDisposition: 'WRITE_TRUNCATE',
            schema: {
                fields: [
                    {name: 'id', type: 'STRING'},
                    {name: 'deviceId', type: 'INTEGER'},
                    {name: 'systemId', type: 'INTEGER'},
                    {name: 'systemDeviceId', type: 'INTEGER'},
                    {name: 'zoneId', type: 'INTEGER'},
                    {name: 'zoneName', type: 'STRING'},
                    {name: 'zoneDirId', type: 'INTEGER'},
                    {name: 'zoneDir', type: 'STRING'},
                    {name: 'permit', type: 'STRING'},
                    {name: 'internalName', type: 'STRING'},
                    {name: 'publicName', type: 'STRING'},
                    {name: 'fullName', type: 'STRING'},
                    {name: 'displayStatusId', type: 'INTEGER'},
                    {name: 'lat', type: 'FLOAT'},
                    {name: 'lon', type: 'FLOAT'},
                    {name: 'locTypeId', type: 'INTEGER'},
                    {name: 'locTypeName', type: 'STRING'},
                    {name: 'temporalType', type: 'STRING'},
                    {name: 'temporalValue', type: 'INTEGER'},
                    {name: 'created', type: 'TIMESTAMP'},
                    {name: 'updated', type: 'TIMESTAMP'},
                    {name: 'retired', type: 'TIMESTAMP'},
                    {name: 'hash', type: 'INTEGER'}
                ],
            },
        };

        // Loads data from a Google Cloud Storage file into the table
        bigquery
            .dataset(datasetId)
            .table(tableId)
            .load(storage.bucket(file.bucket).file(file.name),
                jobMetadata)
            .then(results => {
            job = results[0];
        console.log(`STARTED Job ${job.id} that loads data from gs://${bucketname}/${filename}
                into table ${tableId}.`);

        // Wait for the job to finish
        return job;
    })
    .then(metadata => {
            // Check the job's status for errors
            const errors = metadata.status.errors;
        if (errors && errors.length > 0) {
            throw errors;
        }
    })
    .then(() => {
        console.log(`COMPLETED Job ${job.id} loading data from gs://${bucketname}/${filename} into table ${tableId} .`);

        storage
            .bucket(bucketname)
            .file(filename)
            .delete()
            .then(function(data) {
                const apiResponse = data;
                console.log(`Deleting gs://${bucketname}/${filename} .`);
                console.log("Response");
                console.log(apiResponse);
            });
    })
    .catch(err => {
        console.error(`ERROR DURING Job ${job.id} loading data from gs://${bucketname}/${filename} into table ${tableId} `, err);
        const dateNow = new Date();
        const rows = [{
            filename: filename,
            error: err.toString(),
            date:dateNow
        }];
        // Insert data into a table
        bigquery
            .dataset(datasetId)
            .table("devices_log")
            .insert(rows, insertHandler);
    });

        console.log(`Loading from gs://${data.bucket}/${data.name} into ${datasetId}.${tableId}`);
    }
    else{
        console.log("File Name is not device*, so not processing "+filename);
    }
};


function insertHandler(err, apiResponse) {
    if (err) {
        // An API error or partial failure occurred.
        console.log("Inside insertHandler:"+err);
        console.log(err);
    }
}
