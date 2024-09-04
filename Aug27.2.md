# File Handling JSON

#  Exercise 1: Reading a JSON File
# 1. Create a JSON file named `data.json` with the following content:
import csv
import json
data = {
       "name": "John Doe",
       "age": 30,
       "city": "New York",
       "skills": ["Python", "Machine Learning", "Data Analysis"]
}

with open("C:/Users/chand/Documents/data1.json","w") as file:
    json.dump(data, file)
# 2. Write a Python script to read and print the contents of the JSON file.
with open("C:/Users/chand/Documents/data1.json","r") as file:
    loaded_data = json.load(file)
    print(loaded_data)

# Exercise 2: Writing to a JSON File
# 1. Create a Python dictionary representing a person's profile:
profile = {
       "name": "Jane Smith",
       "age": 28,
       "city": "Los Angeles",
       "hobbies": ["Photography", "Traveling", "Reading"]
}
# 2. Write a Python script to save this data to a JSON file named `profile.json`.
with open("C:/Users/chand/Documents/profile.json","w") as file:
    json.dump(data, file)

# Exercise 3: Converting CSV to JSON
# 1. Using the `students.csv` file from the CSV exercises, write a Python script to read the file and convert the data to a list of dictionaries.
students = []
with open("C:/Users/chand/Documents/data.csv","r") as file:
    reader = csv.DictReader(file)
    for row in reader:
        students.append(row)
# 2. Save the list of dictionaries to a JSON file called `students.json`.
with open("C:/Users/chand/Documents/students.json","w") as file:
    json.dump(students, file)

# Exercise 4: Converting JSON to CSV
# 1. Using the `data.json` file from Exercise 1, write a Python script to read the JSON data.
with open('C:/Users/chand/Documents/data.json', 'r') as json_file:
    json_loaded = json.load(json_file)
print(json_loaded)
# 2. Convert the JSON data to a CSV format and write it to a file named `data.csv`.
with open("C:/Users/chand/Documents/data1.csv", "w", newline='') as file:
    writer = csv.writer(file)
    writer.writerow(json_loaded.keys())
    writer.writerow(json_loaded.values())


# Exercise 5: Nested JSON Parsing
# 1. Create a JSON file named `books.json` with the following content:
data_book = {
       "books": [
           {"title": "The Great Gatsby", "author": "F. Scott Fitzgerald", "year": 1925},
           {"title": "War and Peace", "author": "Leo Tolstoy", "year": 1869},
           {"title": "The Catcher in the Rye", "author": "J.D. Salinger", "year": 1951}
       ]
   }

with open("C:/Users/chand/Documents/books.json","w") as file:
    json.dump(data_book, file)

# 2. Write a Python script to read the JSON file and print the title of each book.

with open("C:/Users/chand/Documents/books.json","r") as file:
    book_data = json.load(file)
for book in book_data["books"]:
    print(book["title"])