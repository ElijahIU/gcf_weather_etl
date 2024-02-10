const {Storage} = require('@google-cloud/storage');
const csv = require('csv-parser');

exports.readObservation = (file, context) => {
    const gcs = new Storage();
    const dataFile = gcs.bucket(file.bucket).file(file.name);

    dataFile.createReadStream()
    .on('error', (error) => {
        console.error(error);
    })
    .pipe(csv())
    .on('data', (row) => {
        // Transform data
        transformData(row);
    })
    .on('end', () => {
        console.log('End!');
    });
}

// Helper function to transform data
function transformData(row) {
    // Convert values to numeric
    for (let key in row) {
        row[key] = parseFloat(row[key]);
    }

    // Transform specific fields
    // Handle null values and scale numeric fields
    const numericFields = ['airtemp', 'dewpoint', 'pressure', 'windspeed', 'precip1hour', 'precip6hour'];
    numericFields.forEach(field => {
        if (row[field] === -9999) {
            row[field] = null; // Rewrite -9999 as null
        } else {
            row[field] /= 10; // Divide by 10
        }
    });

    // Add station id directly
    row['station'] = '724380-93819'; // Hardcoded station id for Indianapolis

    console.log(row); // Output transformed row
}
