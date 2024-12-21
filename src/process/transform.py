import sys
import json
import re

# Function to clean the price - clean the price from special characters and convert it to float
def clean_price(price):
    price = price.replace('&nbsp;', '')
    price = re.split(r'[*\+\-]', price)[0].strip()
    price = re.sub(r'[^\d,.]', '', price)
    price = price.replace(",", ".")
    return float(price)

# Function to convert product name to lowercase
def uniformize_name(name):
    return name.lower().strip()

# Function to convert the stock to boolean value
def stock_to_bool(stock):
    return stock.lower() == 'en stock'

# Function to remove duplicates from the products list
def remove_duplicates(products):
    seen = set()
    unique_products = []
    for product in products:
        identifier = (product['name'], product['price'])
        if identifier not in seen:
            seen.add(identifier)
            unique_products.append(product)
    return unique_products


# Function to calculate the statistics of the products
def calculate_statistics(products):
    # Initialize the brand statistics
    brand_stats = {}

    # Calculate the statistics for each brand
    for product in products:
        brand = product['brand']
        price = float(product['price'])

        # If the brand is not in the brand_stats dictionary, add it
        if brand not in brand_stats:
            brand_stats[brand] = {
                'total_price': 0,
                'min_price': price,
                'max_price': price,
                'total_products': 0
            }

        # Update the statistics for the brand
        brand_stats[brand]['total_price'] += price
        brand_stats[brand]['min_price'] = min(brand_stats[brand]['min_price'], price)
        brand_stats[brand]['max_price'] = max(brand_stats[brand]['max_price'], price)
        brand_stats[brand]['total_products'] += 1

    # Calculate the average price for each brand
    for brand, stats in brand_stats.items():
        stats['average_price'] = stats['total_price'] / stats['total_products']

    return brand_stats

if __name__ == "__main__":
    try:
        # Load the JSON data from the command line argument
        raw_data = sys.argv[1]
        input_data = json.loads(raw_data)

        # Validate the JSON data
        if not isinstance(input_data, dict) or "products" not in input_data:
            raise ValueError("Invalid JSON data: expected an object with a 'products' key.")

        # Get the list of products from the JSON data
        products = input_data["products"]

        # Clean and uniformize the product data
        for product in products:
            if "name" in product:
                product["name"] = uniformize_name(product["name"])
            if "price" in product:
                product["price"] = clean_price(product["price"])
            if "stock" in product:
                product["stock"] = stock_to_bool(product["stock"])

        # Remove duplicates from the list of products
        products = remove_duplicates(products)
        input_data["products"] = products

        # Calculate the statistics of the products
        stats = calculate_statistics(products)
        input_data["statistics"] = stats

        # Print the transformed JSON data
        print(json.dumps(input_data, ensure_ascii=False, indent=4))

    # Handle the exceptions
    except IndexError:
        print("Error: No JSON argument provided to the Python script.", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"Validation error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)