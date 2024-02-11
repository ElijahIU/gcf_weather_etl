const { BigQuery } = require('@google-cloud/bigquery');
const { Storage } = require('@google-cloud/storage');
const csv = require('csv-parser');

// Create a BigQuery client
const bigquery = new BigQuery();

exports.readObservation = (file, context) => {
    const gcs = new Storage();
    const dataFile = gcs.bucket(file.bucket).file(file.name);

    dataFile.createReadStream()
    .on('error', (error) => {
        console.error(error);
    })
    .pipe(csv())
    .on('data', (row) => {
        // Extract station ID from file name
        const stationId = extractStationId(file.name);

        // Transform data
        const transformedRow = transformData(row, stationId);
        
        // Write the transformed data to BigQuery
        writeDataToBigQuery(transformedRow);
    })
    .on('end', () => {
        console.log('End!');
    });
}

// Helper function to extract station ID from file name
function extractStationId(fileName) {
    // Split the file name by '.' and get the first part 
    //According to the solution image only the first part was used
    // Assuming station ID is the first part before the dot
    const parts = fileName.split('.');
    return parts[0]; 
}

// Helper function to transform data
function transformData(row, stationId) {
    // Convert values to numeric
    for (let key in row) {
        row[key] = parseFloat(row[key]);
    }

    // Transform specific fields
    // Handle null values and scale numeric fields
    const numericFields = ['airtemp', 'dewpoint', 'pressure', 'windspeed', 'sky', 'precip1hour', 'precip6hour'];
    numericFields.forEach(field => {
        if (row[field] === -9999) {
            // Rewrite -9999 as null
            row[field] = null; 
        } else {
            // Divide by 10 as per instruction
            row[field] /= 10; 
        }
    });

    // Add station id from file name
    // Use the extracted station ID
    row['station'] = stationId; 

    // Return transformed row
    return row; 
    
}

// Helper function to write data to BigQuery
async function writeDataToBigQuery(row) {
    try {
        // Insert data into a BigQuery table
        const datasetId = 'weather_etl';
        const tableId = 'weather_observation';
        const dataset = bigquery.dataset(datasetId);
        const table = dataset.table(tableId);

        await table.insert([row]);
        console.log('Data inserted into BigQuery');
    } catch (error) {
        console.error('Error inserting data into BigQuery:', error);
    }
}
