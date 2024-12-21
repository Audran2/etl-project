const { chromium } = require('playwright');
const { spawn } = require('node:child_process');
const {insertProduct, updateBrandStats} = require("./load");

// Initialize the browser and create a new page
async function initializeBrowser() {
    const browser = await chromium.launch({ headless: false });
    const page = await browser.newPage();
    return { browser, page };
}

// Extract product links from the current page
async function getProductLinks(page) {
    return await page.$$eval('.pdt-item .listing-product__head .listing-product__infos .listing-product__desc', (links) => links.map((link) => link.href));
}

// Extract product details from the product page
async function getProductDetails(page) {
    return await page.evaluate(() => {
        // Helper function to get the detail value based on the label text
        const getDetail = (labelText) => {
            const label = Array.from(document.querySelectorAll('td.label h3'))
                .find((el) => el.textContent.trim() === labelText);
            if (label) {
                const valueCell = label.closest('td').nextElementSibling;
                return valueCell ? valueCell.textContent.trim() : "N/A";
            }
            return "N/A";
        };

        return {
            brand: getDetail('Marque'),
            processor: getDetail('Processeur'),
            gpu: getDetail('Chipset graphique'),
            ram: getDetail('Fréquence(s) Mémoire'),
            screen_size: getDetail('Taille standard d\'écran d\'ordinateur (en in)'),
        };
    });
}

// Extract product information from the product page
async function getProductInfo(page) {
    const name = await page.$('.product__title');
    const price = await page.$('.price');
    const stock = await page.$('.product__stock .modal-stock-web');
    const desc = await page.$('.product__description__subtitle');
    const image = await page.$('.product-image a img');

    // Extract text content from the elements
    const nameText = name ? (await name.textContent()).trim() : '';
    let priceText = '';
    if (price) {
        const newPrice = await price.$('.new-price');
        if (newPrice) {
            priceText = (await newPrice.textContent()).trim();
        } else {
            priceText = (await price.textContent()).trim();
        }
    }
    const stockText = stock ? (await stock.textContent()).trim() : '';
    const descText = desc ? (await desc.textContent()).trim() : '';
    const imageUrl = image ? (await image.getAttribute('src')) : '';

    return { name: nameText, price: priceText, stock: stockText, description: descText, image: imageUrl };
}

// Scrape all products from the current page
async function scrapePageProducts(page) {
    const productLinks = await getProductLinks(page);
    const products = [];

    // Loop through each product link and extract the product information
    for (const link of productLinks) {
        await page.goto(link);
        // Add a delay to ensure we don't get blocked by the website
        await page.waitForTimeout(2000);

        const productInfo = await getProductInfo(page);
        const productDetails = await getProductDetails(page);

        const product = { ...productInfo, ...productDetails };
        products.push(product);
    }

    return products;
}

// Process the extracted products using a Python script
async function processWithPython(products) {
    const pythonScriptPath = './src/process/transform.py';
    const pythonProcess = spawn('python3', [pythonScriptPath, JSON.stringify({ products })]);

    let result = '';
    let error = '';

    // Capture the output and error messages from the Python script
    pythonProcess.stdout.on('data', (data) => {
        result += data.toString();
    });

    pythonProcess.stderr.on('data', (data) => {
        error += data.toString();
    });

    // Return a promise that resolves when the Python script completes
    return new Promise((resolve, reject) => {
        pythonProcess.on('close', (code) => {
            if (code === 0) {
                try {
                    const parsedResult = JSON.parse(result);
                    resolve({
                        products: parsedResult.products,
                        statistics: parsedResult.statistics
                    });
                } catch (e) {
                    reject(`JSON error from Python script: ${e.message}`);
                }
            } else {
                reject(`Python script exited with code ${code}\nPython error: ${error}`);
            }
        });
    });
}

// Main function to extract data from a given page number
async function dataExtraction(pageNumber) {
    console.log(`Running data extraction for page ${pageNumber}...`);
    // Construct the URL for the given page number
    const pageUrl = pageNumber === 1
        ? 'https://www.rueducommerce.fr/r/70151/pc-portable/'
        : `https://www.rueducommerce.fr/r/70151/pc-portable/p${pageNumber}`;

    try {
        const { browser, page } = await initializeBrowser();

        await page.goto(pageUrl, { waitUntil: 'load' });
        await page.waitForTimeout(2000);

        // Scrape all products from the page
        const allProducts = await scrapePageProducts(page);
        // Process the extracted data using a Python script
        const processedData = await processWithPython(allProducts);

        // Insert the processed products and update brand statistics
        for (const product of processedData.products) {
            await insertProduct(product);
        }

        for (const brand in processedData.statistics) {
            const stats = processedData.statistics[brand];
            await updateBrandStats(brand, stats);
        }

        console.log(`Data extraction and processing completed for page ${pageNumber}`);
        await browser.close();
    } catch (error) {
        console.error(`Error during data extraction for page ${pageNumber}:`, error);
    }
}

module.exports = dataExtraction;
