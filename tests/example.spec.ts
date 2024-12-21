import {test} from '@playwright/test';
import {createObjectCsvWriter} from "csv-writer";

test.only('scrap TopAchat with Ref extraction', async ({ page }) => {
    test.setTimeout(0);

    await page.goto('https://www.topachat.com/pages/produits_cat_est_micro_puis_rubrique_est_wpr.html');

    // Extract product links from the listing page
    let productLinks = await page.$$eval('.product-list__product-wrapper a', links => {
        return links.map(link => (link as HTMLAnchorElement).href);
    });

    // Optionally, limit the number of products to scrape for testing
    // productLinks = productLinks.slice(0, 10);

    const products: { type: string, libelle: string, price: number, marque: string }[] = [];

    for (const link of productLinks) {
        try {
            await page.goto(link);

            // Extract product title
            const title = await page.$('.ps-main__product-title');
            let titleText = title ? (await title.textContent())?.trim() || '' : '';

            // Remove all text inside parentheses (including the parentheses themselves)
            titleText = titleText.replace(/\(.*?\)/g, '').trim();

            // Extract product price
            const price = await page.$('.offer-price__price');
            let priceValue = 0;
            if (price) {
                const priceText = (await price.textContent())?.trim().replace(/\u00A0/g, '').replace('€', '') || '';
                priceValue = parseFloat(priceText); // Convert to a numeric value
            }

            let marque = '';

            // Navigate to "Fiche Technique" tab
            const ficheTechniqueTab = await page.locator('a.ps-sheet__tablink-a:has-text("FICHE TECHNIQUE")');
            if (await ficheTechniqueTab.isVisible()) {
                await ficheTechniqueTab.click();

                // Wait for the content of the "Fiche Technique" tab to load
                // await page.waitForSelector('.fiche-technique');

                // Extract the "Ref" div to determine the brand
                const ref = await page.$('.ps-sheet__warranty div:has-text("Ref")');
                const refText = ref ? (await ref.textContent())?.trim() || '' : '';

                // Use a regular expression to extract text between 'Ref' and ':'
                const match = refText.match(/Ref\s+([^\:]+)/);
                marque = match ? match[1].trim() : '';
            }

            // Push extracted data to products array
            products.push({ type: "CPU", libelle: titleText, price: priceValue, marque });
        } catch (error) {
            console.error(`Error scraping product link: ${link}`, error);
        }
    }

    // Write extracted data to CSV
    const csvWriter = createObjectCsvWriter({
        path: 'topachat_products.csv',
        header: [
            { id: 'type', title: 'Type'},
            { id: 'libelle', title: 'Libelle' },
            { id: 'price', title: 'Price' },
            { id: 'marque', title: 'Marque' },
        ],
        append: false, // Overwrite the file if it exists
    });

    await csvWriter.writeRecords(products);
    console.log('TopAchat Data written to CSV file');
});