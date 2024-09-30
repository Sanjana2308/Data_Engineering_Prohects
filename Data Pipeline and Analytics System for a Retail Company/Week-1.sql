use RetailProject

-- Customer Dimension
CREATE TABLE customer_dim ( 
    customer_id INT PRIMARY KEY, 
    customer_name VARCHAR(255), 
    email VARCHAR(255), 
    phone_number VARCHAR(20),
    location VARCHAR(255), 
    registration_date DATE
);

-- Product Dimension
CREATE TABLE product_dim ( 
    product_id INT PRIMARY KEY, 
    product_name VARCHAR(255), 
    category VARCHAR(255), 
    brand VARCHAR(255), 
    price DECIMAL(10, 2)
);

-- Store Dimension
CREATE TABLE store_dim ( 
    store_id INT PRIMARY KEY, 
    store_name VARCHAR(255), 
    address VARCHAR(255), 
    city VARCHAR(255), 
    state VARCHAR(255), 
    country VARCHAR(255)
);

-- Date Dimension (used for time-related analytics)
CREATE TABLE date_dim (
    date_id INT PRIMARY KEY,
    full_date DATE,
    day_of_week VARCHAR(10),
    month VARCHAR(20),
    year INT,
    quarter VARCHAR(10)
);

-- Sales Fact Table
CREATE TABLE sales_fact ( 
    sales_id INT PRIMARY KEY, 
    product_id INT, 
    customer_id INT, 
    store_id INT, 
    date_id INT,
    quantity INT, 
    sales_amount DECIMAL(10, 2), 
    discount DECIMAL(5, 2),
    order_date DATE,
    FOREIGN KEY (product_id) REFERENCES product_dim(product_id),
    FOREIGN KEY (customer_id) REFERENCES customer_dim(customer_id),
    FOREIGN KEY (store_id) REFERENCES store_dim(store_id),
    FOREIGN KEY (date_id) REFERENCES date_dim(date_id)
);



INSERT INTO customer_dim (customer_id, customer_name, email, phone_number, location, registration_date) VALUES
(1, 'John Doe', 'johndoe@example.com', '1234567890', 'New York', '2022-01-15'),
(2, 'Jane Smith', 'janesmith@example.com', '0987654321', 'Los Angeles', '2022-03-22'),
(3, 'Alice Brown', 'aliceb@example.com', '4567891230', 'Chicago', '2022-04-18'),
(4, 'Bob Johnson', 'bobjohnson@example.com', '7890123456', 'Houston', '2022-06-05'),
(5, 'Charlie Davis', 'charlied@example.com', '3216549870', 'Phoenix', '2022-07-10'),
(6, 'Emily White', 'emilyw@example.com', '2345678901', 'San Francisco', '2022-08-22'),
(7, 'Daniel Green', 'danielg@example.com', '5678901234', 'Boston', '2022-09-30'),
(8, 'Sarah Black', 'sarahb@example.com', '8901234567', 'Miami', '2022-11-03'),
(9, 'Michael Grey', 'michaelg@example.com', '1230987654', 'Denver', '2022-12-15'),
(10, 'Sophia Blue', 'sophiab@example.com', '9087654321', 'Seattle', '2023-01-05');

INSERT INTO product_dim (product_id, product_name, category, brand, price) VALUES
(1, 'Laptop', 'Electronics', 'Dell', 999.99),
(2, 'Smartphone', 'Electronics', 'Apple', 799.99),
(3, 'Headphones', 'Accessories', 'Sony', 199.99),
(4, 'Desk Chair', 'Furniture', 'Ikea', 149.99),
(5, 'Coffee Maker', 'Appliances', 'Breville', 129.99),
(6, 'Tablet', 'Electronics', 'Samsung', 499.99),
(7, 'Microwave', 'Appliances', 'LG', 89.99),
(8, 'Monitor', 'Electronics', 'HP', 199.99),
(9, 'Smartwatch', 'Electronics', 'Fitbit', 149.99),
(10, 'Blender', 'Appliances', 'Vitamix', 299.99);

INSERT INTO store_dim (store_id, store_name, address, city, state, country) VALUES
(1, 'Best Buy', '123 Tech Lane', 'New York', 'NY', 'USA'),
(2, 'Apple Store', '456 Innovation Blvd', 'Los Angeles', 'CA', 'USA'),
(3, 'Walmart', '789 Budget St', 'Chicago', 'IL', 'USA'),
(4, 'Target', '101 Everyday Rd', 'Houston', 'TX', 'USA'),
(5, 'Ikea', '202 Furniture Ave', 'Phoenix', 'AZ', 'USA'),
(6, 'Costco', '305 Wholesale Dr', 'San Francisco', 'CA', 'USA'),
(7, 'Home Depot', '180 Tool St', 'Boston', 'MA', 'USA'),
(8, 'Kroger', '333 Grocery Ln', 'Miami', 'FL', 'USA'),
(9, 'Staples', '910 Office Supply Rd', 'Seattle', 'WA', 'USA'),
(10, 'CVS Pharmacy', '455 Health Way', 'Denver', 'CO', 'USA');


INSERT INTO date_dim (date_id, full_date, day_of_week, month, year, quarter) VALUES
(1, '2022-01-15', 'Saturday', 'January', 2022, 'Q1'),
(2, '2022-03-22', 'Tuesday', 'March', 2022, 'Q1'),
(3, '2022-04-18', 'Monday', 'April', 2022, 'Q2'),
(4, '2022-06-05', 'Sunday', 'June', 2022, 'Q2'),
(5, '2022-07-10', 'Sunday', 'July', 2022, 'Q3'),
(6, '2022-08-22', 'Monday', 'August', 2022, 'Q3'),
(7, '2022-09-30', 'Friday', 'September', 2022, 'Q3'),
(8, '2022-11-03', 'Thursday', 'November', 2022, 'Q4'),
(9, '2022-12-15', 'Thursday', 'December', 2022, 'Q4'),
(10, '2023-01-05', 'Thursday', 'January', 2023, 'Q1');


INSERT INTO sales_fact (sales_id, product_id, customer_id, store_id, date_id, quantity, sales_amount, discount, order_date) VALUES
(1, 1, 1, 1, 1, 2, 1999.98, 100.00, '2022-01-15'),
(2, 2, 2, 2, 2, 1, 799.99, 50.00, '2022-03-22'),
(3, 3, 3, 3, 3, 3, 599.97, 30.00, '2022-04-18'),
(4, 4, 4, 4, 4, 1, 149.99, 10.00, '2022-06-05'),
(5, 5, 5, 5, 5, 1, 129.99, 5.00, '2022-07-10'),
(6, 6, 6, 6, 6, 1, 499.99, 20.00, '2022-08-22'),
(7, 7, 7, 7, 7, 2, 179.98, 15.00, '2022-09-30'),
(8, 8, 8, 8, 8, 2, 399.98, 25.00, '2022-11-03'),
(9, 9, 9, 9, 9, 1, 149.99, 10.00, '2022-12-15'),
(10, 10, 10, 10, 10, 1, 299.99, 20.00, '2023-01-05');


select * from customer_dim
select * from product_dim
select * from store_dim
select * from date_dim
select * from sales_fact

-- Queries

-- Total sales amount by product
SELECT p.product_name, SUM(s.sales_amount) AS total_sales
FROM sales_fact s
JOIN product_dim p ON s.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_sales DESC;

-- Total sales quanitity by coustomer
SELECT c.customer_name, SUM(s.quantity) AS total_quantity
FROM sales_fact s
JOIN customer_dim c ON s.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_quantity DESC;

-- Average discount by store
SELECT st.store_name, AVG(s.discount) AS avg_discount
FROM sales_fact s
JOIN store_dim st ON s.store_id = st.store_id
GROUP BY st.store_name
ORDER BY avg_discount DESC;

-- Total Sales by product category
SELECT p.category, SUM(s.sales_amount) AS total_sales
FROM sales_fact s
JOIN product_dim p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sales DESC;

-- Top Best selling products
SELECT p.product_name, SUM(s.quantity) AS total_quantity
FROM sales_fact s
JOIN product_dim p ON s.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_quantity DESC

-- Total sales by Region
SELECT st.country, SUM(s.sales_amount) AS total_sales
FROM sales_fact s
JOIN store_dim st ON s.store_id = st.store_id
GROUP BY st.country
ORDER BY total_sales DESC;

-- Avg sales amount prt order by store
SELECT st.store_name, AVG(s.sales_amount) AS avg_sales_per_order
FROM sales_fact s
JOIN store_dim st ON s.store_id = st.store_id
GROUP BY st.store_name
ORDER BY avg_sales_per_order DESC;

-- Sales by quarter
SELECT d.quarter, SUM(s.sales_amount) AS total_sales
FROM sales_fact s
JOIN date_dim d ON s.date_id = d.date_id
GROUP BY d.quarter
ORDER BY total_sales DESC;
