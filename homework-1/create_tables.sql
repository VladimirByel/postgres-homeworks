-- SQL-команды для создания таблиц

CREATE TABLE employees
(
	employee_id int PRIMARY KEY,
	first_name varchar(100) NOT NULL,
	last_name varchar(100) NOT NULL,
	title varchar(100) NOT NULL,
	birth_date char(10),
	notes text
);

CREATE TABLE customers
(
	customer_id varchar(6) PRIMARY KEY,
	company_name varchar(100) NOT NULL,
	contact_name varchar(100) NOT NULL
);

CREATE TABLE orders
(
	order_id int PRIMARY KEY,
	customer_id varchar(6) REFERENCES customers(customer_id),
	employee_id int REFERENCES employees(employee_id),
	order_date char(10),
	ship_city varchar(100)
);
