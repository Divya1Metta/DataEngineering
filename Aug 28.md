
# Pandas Exercise

import pandas as pd


# Creating a new dataset
data = {
    "Employee_ID": [101, 102, 103, 104, 105, 106],
    "Name": ["Rajesh", "Meena", "Suresh", "Anita", "Vijay", "Neeta"],
    "Department": ["HR", "IT", "Finance", "IT", "Finance", "HR"],
    "Age": [29, 35, 45, 32, 50, 28],
    "Salary": [70000, 85000, 95000, 64000, 120000, 72000],
    "City": ["Delhi", "Mumbai", "Bangalore", "Chennai", "Delhi", "Mumbai"]
}

df = pd.DataFrame(data)
# print(df)

# Exercise 1: Rename Columns
# Rename the "Salary" column to "Annual Salary" and "City" to "Location".
dfRenamed = df.rename(columns={"Salary":"Annual Salary","City":"Location"})

# Print the updated DataFrame
print(dfRenamed)

# Exercise 2: Drop Columns
# Drop the "Location" column from the DataFrame.
new_df_dropped = dfRenamed.drop(columns=["Location"])
# Print the DataFrame after dropping the column
print(new_df_dropped)

# Exercise 3: Drop Rows
# Drop the row where "Name" is "Suresh".
df_row_dropped = dfRenamed.drop(df[dfRenamed["Name"] == "Suresh"].index)
# Print the updated DataFrame.
print(df_row_dropped)

# Exercise 4: Handle Missing Data
# Assign None to the "Salary" of "Meena".
dfRenamed.loc[dfRenamed["Name"]=="Meena","Annual Salary"] = None
# # Fill the missing "Salary" value with the mean salary of the existing employees.
mean_salary = dfRenamed["Annual Salary"].mean()
dfRenamed["Annual Salary"] = dfRenamed["Annual Salary"].fillna(mean_salary)
# Print the cleaned DataFrame
print(dfRenamed)

# Exercise 5: Create Conditional Columns
# Create a new column "Seniority" that assigns "Senior" to employees aged 40 or above and "Junior" to employees younger than 40.
df["Seniority"] = df["Age"].apply(lambda x: "Senior" if x >= 40 else "Junior")
# Print the updated DataFrame
print(df)

# Exercise 6: Grouping and Aggregation
# Group the DataFrame by "Department" and calculate the average salary in each department.
df_grouped_dept = df.groupby("Department")["Salary"].mean()
# Print the grouped DataFrame.
print(df_grouped_dept)
