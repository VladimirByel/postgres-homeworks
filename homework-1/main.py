"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
from config import KEY
import csv

with psycopg2.connect(host='localhost', database='postgres', user='postgres', password=KEY) as connection:
    with connection.cursor() as cursor:
        with open("north_data/customers_data.csv", newline='') as file:
            reads = csv.reader(file)
            for read in reads:
                if read[0] != "customer_id":
                    cursor.execute("INSERT INTO customers VALUES (%s, %s, %s)", (read[0], read[1], read[2]))

        with open("north_data/employees_data.csv", newline='') as file:
            readers = csv.reader(file)
            for reader in readers:
                if reader[0] != "employee_id":
                    cursor.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                   (reader[0], reader[1], reader[2], reader[3], reader[4], reader[5]))

        with open("north_data/orders_data.csv", newline='') as file:
            peruse = csv.reader(file)
            for perusing in peruse:
                if perusing[0] != "order_id":
                    cursor.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                   (perusing[0], perusing[1], perusing[2], perusing[3], perusing[4]))


connection.close()
