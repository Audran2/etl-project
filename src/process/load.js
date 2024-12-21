const cassandra = require('cassandra-driver');
require('dotenv').config();

let client;

// Initialize the Cassandra client and create the necessary tables
async function initializeCassandra() {
    try {
        client = new cassandra.Client({
            contactPoints: [process.env.CASSANDRA_CONTACT_POINTS],
            localDataCenter: process.env.CASSANDRA_LOCAL_DATA_CENTER,
            keyspace: process.env.CASSANDRA_KEYSPACE,
        });

        await client.connect();
        console.log('Connected to Cassandra!');

        // Create the products table if it doesn't exist
        const createTableQuery = `
            CREATE TABLE IF NOT EXISTS products (
                id UUID PRIMARY KEY,
                name TEXT,
                price decimal,
                brand TEXT,
                stock BOOLEAN,
                description TEXT,
                image TEXT,
                processor TEXT,
                gpu TEXT,
                ram TEXT,
                screen_size TEXT,
                scraped_at TIMESTAMP
            );
        `;

        // Create the brand_stats table if it doesn't exist
        const createBrandStatsTableQuery = `
            CREATE TABLE IF NOT EXISTS brand_stats (
               brand TEXT PRIMARY KEY,
               average_price DECIMAL,
               min_price DECIMAL,
               max_price DECIMAL,
               total_products INT,
               last_updated TIMESTAMP
            );
        `;

        await client.execute(createTableQuery);
        console.log('Table "products" initialized.');

        await client.execute(createBrandStatsTableQuery);
        console.log('Table "brand_stats" initialized.');
    } catch (error) {
        console.error('Error initializing Cassandra:', error);
        throw error;
    }
}

// Insert a product into the products table
async function insertProduct(product) {
    try {
        const query = `
            INSERT INTO products (id, name, price, brand, stock, description, image, processor, gpu, ram, screen_size, scraped_at)
            VALUES (uuid(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()));
        `;

        const params = [
            product.name,
            product.price,
            product.brand,
            product.stock,
            product.description,
            product.image,
            product.processor,
            product.gpu,
            product.ram,
            product.screen_size
        ];

        // Insert the product into the products table
        await client.execute(query, params, { prepare: true });
        console.log('Product inserted:', product.name);
    } catch (error) {
        console.error('Error inserting product:', error);
    }
}

// Update the brand statistics in the brand_stats table
const updateBrandStats = async (brand, stats) => {
    console.log(`Updating stats for brand: ${brand}`);

    const { total_price, min_price, max_price, total_products, average_price } = stats;

    // Check if the brand exists in the brand_stats table
    const query = `
        SELECT * FROM brand_stats WHERE brand = ?
    `;
    const result = await client.execute(query, [brand], { prepare: true });

    // If the brand doesn't exist, insert a new row
    if (result.rowLength === 0) {
        const insertStatsQuery = `
            INSERT INTO brand_stats (brand, average_price, min_price, max_price, total_products, last_updated)
            VALUES (?, ?, ?, ?, ?, toTimestamp(now()))
        `;
        await client.execute(insertStatsQuery, [brand, average_price, min_price, max_price, total_products], { prepare: true });
    } else {
        // If the brand exists, update the existing row
        const currentStats = result.rows[0];

        const newAvgPrice = (currentStats.total_products * currentStats.average_price + total_price) / (currentStats.total_products + total_products);
        const newMinPrice = Math.min(currentStats.min_price, min_price);
        const newMaxPrice = Math.max(currentStats.max_price, max_price);
        const newTotalProducts = currentStats.total_products + total_products;

        const updateStatsQuery = `
            UPDATE brand_stats
            SET average_price = ?, min_price = ?, max_price = ?, total_products = ?, last_updated = toTimestamp(now())
            WHERE brand = ?
        `;
        // Update the brand statistics
        await client.execute(updateStatsQuery, [newAvgPrice, newMinPrice, newMaxPrice, newTotalProducts, brand], { prepare: true });
    }
};

// Retrieve all products from the products table
async function getProducts() {
    try {
        const query = 'SELECT * FROM products;';
        const result = await client.execute(query);
        return result.rows;
    } catch (error) {
        console.error('Error retrieving products:', error);
        throw error;
    }
}

// Retrieve all brand statistics from the brand_stats table
async function getBrandStatistics() {
    try {
        const query = 'SELECT * FROM brand_stats;';
        const result = await client.execute(query);
        return result.rows;
    } catch (error) {
        console.error('Error retrieving brand statistics:', error);
        throw error;
    }
}

module.exports = {
    initializeCassandra,
    insertProduct,
    updateBrandStats,
    getProducts,
    getBrandStatistics
};
