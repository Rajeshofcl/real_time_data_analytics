import pandas as pd
import random

# Define categories and their respective subcategories
categories = {
    'Electronics': ['Smartphones', 'Laptops', 'Televisions', 'Cameras'],
    'Clothing': ['Shoes', 'Shirts', 'Pants', 'Hats'],
    'Furniture': ['Sofas', 'Tables', 'Chairs', 'Beds'],
    'Beauty': ['Makeup', 'Skincare', 'Haircare', 'Fragrance'],
    'Sports': ['Tennis', 'Football', 'Basketball', 'Gym Equipment'],
    'Books': ['Fiction', 'Non-Fiction', 'Comics', 'Magazines'],
    'Toys': ['Action Figures', 'Dolls', 'Board Games', 'Puzzles'],
    'Automobiles': ['Car Accessories', 'Bike Accessories', 'Tyres', 'Engines']
}

rows = 100000
availability_status = ['In Stock', 'Out of Stock']
discounts = ['5%', '10%', '15%', '20%', '25%', '30%']

# Initialize lists to store the generated data
category_ids = []
category_names = []
subcategory_names = []
parent_categories = []
product_ids = []
product_prices = []
discounts_list = []
availability_status_list = []

# Generate 100,000 rows of data with correct category-subcategory matching
for i in range(1, rows + 1):
    category_name = random.choice(list(categories.keys()))  # Select a category
    subcategory_name = random.choice(categories[category_name])  # Select a matching subcategory
    
    category_ids.append(i % len(categories) + 1)  # Cycle through category IDs
    category_names.append(category_name)
    subcategory_names.append(subcategory_name)
    parent_categories.append(None)  # No parent category in this case
    product_ids.append(i)
    product_prices.append(round(random.uniform(10.0, 1000.0), 2))  # Random price
    discounts_list.append(random.choice(discounts))
    availability_status_list.append(random.choice(availability_status))

# Create a DataFrame
product_categories_data = {
    'Category_ID': category_ids,
    'Category_Name': category_names,
    'Subcategory_Name': subcategory_names,
    'Parent_Category': parent_categories,
    'Product_ID': product_ids,
    'Product_Price': product_prices,
    'Discount': discounts_list,
    'Availability_Status': availability_status_list
}

# Convert the dictionary into a DataFrame and save it as a CSV file
df = pd.DataFrame(product_categories_data)
df.to_csv('Updated_Product_Categories.csv', index=False)

print("Updated_Product_Categories.csv has been generated successfully.")
