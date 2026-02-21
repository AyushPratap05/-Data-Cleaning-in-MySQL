# Data Cleaning in MySQL

## 📌 Project Overview
This project focuses on cleaning and standardizing a real-world layoffs dataset using MySQL.
The goal was to convert raw, inconsistent data into analysis-ready structured data.

## 🧩 Dataset Description
- Dataset: Layoffs data
- Common issues:
  - NULL and empty values
  - Duplicate company records
  - Inconsistent industry names

## 🛠️ Tools & Technologies
- MySQL 8+/9.1
- MySQL Workbench
- SQL (Joins, Window Functions, Updates)
- Git & GitHub

## 🔍 Data Cleaning Steps
1. Removed duplicate records using self joins
2. Handled NULL and empty values
3. Standardized industry names
4. Used ROW_NUMBER() to identify duplicates
5. Validated cleaned data

## 📂 Project Structure
- `data/` → raw and cleaned datasets
- `sql/` → SQL scripts used for cleaning
- `screenshots/` → before/after results

## ✅ Outcome
- Cleaned and analysis-ready dataset
- Improved data consistency and accuracy

## 📈 Next Steps
- Exploratory Data Analysis (EDA)
- Power BI dashboard
