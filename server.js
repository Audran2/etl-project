const express = require('express')
const bodyParser = require('body-parser')
const cron = require('node-cron')
const cors = require('cors');
const { initializeCassandra, getProducts, getBrandStatistics } = require('./src/process/load');
const dataExtraction = require("./src/process/extract");

// const for the current page and max page used in the cron job
let currentPage = 1;
let maxPage = 50;

const app = express()

app.use(cors());
app.use(bodyParser.json())

// Initialize Cassandra
initializeCassandra().then(() => {
    console.log('Cassandra is ready to use.');
}).catch((error) => {
    console.error('Failed to initialize Cassandra:', error);
    process.exit(1);
});

// Schedule the cron job to run every 10 minutes
cron.schedule('*/5 * * * *', async () => {
    console.log(`Scheduled scraping for page ${currentPage}...`);
    await dataExtraction(currentPage);

    // Update the current page after scraping
    currentPage = currentPage < maxPage ? currentPage + 1 : 1;
});

// Routes for the API endpoints to get products and brand statistics
app.get('/products', async (req, res) => {
    try {
        const products = await getProducts();
        res.json({ products });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/brands/statistics', async (req, res) => {
    try {
        const statistics = await getBrandStatistics();
        res.json({ statistics });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

const PORT = 3003
app.listen(PORT, async () => {
    console.log(`Server is running on port ${PORT}`);
});
