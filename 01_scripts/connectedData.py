import requests
import json
import mysql.connector as mariadb

# get data from API
response_API = requests.get(
    'https://data.calgary.ca/resource/44h7-jkm5.json')
print("Status code ", response_API.status_code)
data = response_API.text
dataSet = json.loads(data)
print("dataSet about API:\n", dataSet)


# create mysql connection
mariadb_connection = mariadb.connect(
    user='root', password='password', host='localhost', port='3306', database='Family_Garbage_Composition')

# create cursor
create_cursor = mariadb_connection.cursor()

# create the database if it doesn't exist
create_cursor.execute(
    "CREATE DATABASE IF NOT EXISTS Family_Garbage_Composition")

# switch to the database
create_cursor.execute("USE Family_Garbage_Composition")


# Drop table if it already exist using execute() method.
create_cursor.execute("DROP TABLE IF EXISTS Single_and_Multi_Family")

# create table
create_cursor.execute("CREATE TABLE Single_and_Multi_Family (sector_category VARCHAR(255), material_category VARCHAR(255), material_subcategory VARCHAR(255), material_diversion_potential VARCHAR(255), proper_disposal_location VARCHAR(255), weight DOUBLE(2,2))")

# commit the changes
mariadb_connection.commit()


for columns in dataSet:
    dictionary = {
        'sector_category': columns.get('sector_category', ''),
        'material_category': columns.get('material_category', ''),
        'material_subcategory': columns.get('material_subcategory', ''),
        'material_diversion_potential': columns.get('material_diversion_potential', ''),
        'proper_disposal_location': columns.get('proper_disposal_location', ''),
        'weight': columns.get('weight', '')
    }

    # insert the data into the table
    create_cursor.execute(
        "INSERT INTO Single_and_Multi_Family (sector_category,material_category, material_subcategory,  material_diversion_potential, proper_disposal_location, weight) VALUES (%(sector_category)s, %(material_category)s, %(material_subcategory)s, %(material_diversion_potential)s, %(proper_disposal_location)s, %(weight)s )", dictionary)


# # commit the changes
mariadb_connection.commit()


# close the cursor and connection
create_cursor.close()
mariadb_connection.close()
