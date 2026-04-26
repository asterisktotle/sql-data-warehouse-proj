Data Dictionary For Gold Layer

1. gold.dim_customers
  - Purpose: Stores customer details enriched with demographic and geographic data.
  - Columns: 
| Column Name | Data Type | Description|
| :--- | :--- | :--- |
|customer_key|INT|Surrigate key uniquely indentifying each customer record in the dimension table.|
|customer_id|INT|Unique numerical indentifier assigned to each customer.|
|customer_number|NVARCHAR(50)|Alphanumeric identifier representing the customer, used for tracking and referencing.|
|first_name|NVARCHAR(50)|Customer's first name|
|last_name|NVARCHAR(50)|Customer's last name|
|country|NVARCHAR(50)|Customers's country of residence (e.g,. 'Australia')|
|marital_status| NVARCHAR(50) |Marital status of the customer (e.g., 'Married', 'Single')  |
|gender|NVARCHAR(50)|Gender of the customer (e.g., ('Male','Female','n/a')|
|birthdate|DATE|Customer's birtdate formatted as YYYY-MM-DD (e.g., '2002-01-01')|
|creating_date|DATE|Date and time when the customer record was created in the system.|

----------------------------------------------------------------------------------------

2. gold.dim_products
- Purpose: Provides information about the product and their attributes.
- Columns:
| **Column Name** | **Data Type** | **Description **|
| :--- | :--- | :--- |
|product_key|INT|Surrogate key uniquely indentifying each product record in the dimension table.|
|product_id|INT|Unique numerical indentifier assigned to each product.|
|product_name|NVARCHAR(50)|Descriptive name of the product, including key details such as type, color and size.|
|category_id|NVARCHAR(50)|Unique numerical indentifier for the product's category, linking to its high-level classification|
|category|NVARCHAR(50)|broader classification of the product (e.g., Bikes, Components) to the group related items.|
|sub-category|NVARCHAR(50)|A more detailed classification of the product within the category, such asa product type.|
|maintenance_required| NVARCHAR(50) Indicates whether the product requires maintenance (e.g., 'Yes','No')|
|cost|INT|The cost or base price of the product measured in monetary units.|
|product_line|NVARCHAR(50)|The specific product line or series to which the product belongs (e.g., Road, Mountains)|
|start_date|DATE|Date and time the product become available for sale or use.|

3. gold.fact_sales
- Purpose: Stores transactional sales data for analytical purposes.
- Columns:
| **Column Name** | **Data Type** | **Description **|
| :--- | :--- | :--- |
|ordder_number|NVARCHAR(50)|Alphanumeric identifier for each sales order (e.g., 'SO54496')|
|product_key|INT|Surrogate key linking the oder to the product dimension table.|
|customer_key|INT|Surrogate key linkin the order to the customer dimension table.|
|order_date|DATE|Date when the order was placed|
|shipping_date|DATE|Date when the order was shipped to the customer|
|due_date|DATE|Date when the order payment was due.|
|sales_amount| INT|The total monetary value of the sales for the line item, in whole currency unit (e.g., 25)|
|quantity|INT|The number of units of the product ordered for the line item (e.g., 1)|
|price|INT|The price per unit of the product for the line item, in whole currency units.|

