# Cheese Analyzer Hadoop

## Overview

Cheese Analyzer Hadoop is a Java-based application designed to process and analyze cheese data using Hadoop. The application provides an interactive graphical interface for filtering and visualizing data related to various types of cheese, enabling users to gain insights into manufacturing trends, organic production, and moisture content.

## Features

- **Interactive GUI:** Easily filter data based on manufacturer province, cheese category, milk type, and calculation type.
- **Data Analysis:**
  - **Average Moisture Percentage:** Calculate the average moisture content for selected cheese categories and milk types.
  - **Organic Percentage:** Determine the percentage of organic cheeses relative to all cheeses in a selected category and province.
- **Hadoop Integration:** Leverages Hadoop's powerful processing capabilities to handle large datasets efficiently.
- **Visualization:** Presents analysis results in a user-friendly table format for easy interpretation.

## Data Structure

The dataset used by Cheese Analyzer Hadoop includes the following columns:

- **CheeseId:** Unique identifier of the cheese.
- **ManufacturerProvCode:** Manufacturer province code (e.g., AB, NS).
- **ManufacturingTypeEn:** Manufacturing type (e.g., Artisan, Industrial, Farmstead).
- **MoisturePercent:** Moisture percentage of the cheese.
- **FlavourEn:** Description of flavor characteristics.
- **CharacteristicsEn:** Additional characteristics of the cheese.
- **Organic:** Indicator of whether the cheese is organic (1 for organic, 0 for non-organic).
- **CategoryTypeEn:** Category of the cheese (e.g., Fresh Cheese, Semi-soft Cheese).
- **MilkTypeEn:** Type of milk (e.g., Cow, Ewe).
- **MilkTreatmentTypeEn:** Milk treatment type (e.g., Pasteurized, Thermised).
- **RindTypeEn:** Cheese rind type.
- **CheeseName:** Name of the cheese.
- **FatLevel:** Fat level (e.g., higher fat, lower fat).

## Usage

Upon launching the application, the graphical interface will present four filters:

1. **Manufacturer Province Code:** Select from available province codes (e.g., AB, NS).
2. **Category Type:** Choose the cheese category (e.g., Fresh Cheese, Semi-soft Cheese, Hard Cheese).
3. **Milk Type:** Select the type of milk used (e.g., Cow, Ewe).
4. **Calculation Type:** Choose between:
   - **Average Moisture Percent:** Calculates the average moisture percentage for the selected filters.
   - **Organic Percentage:** Calculates the percentage of organic cheeses relative to all cheeses in the selected category and province.

After selecting the desired filters, the results will be displayed in the table below the filters.

## Project Structure

- `src/main/java/fmi/cheeseanalyzer_ErayAli_2101321032/`
  - `mapper/` - Contains Mapper classes for different analysis types.
  - `reducer/` - Contains Reducer classes corresponding to each Mapper.
  - `App.java` - Main application class that initializes the GUI and handles user interactions.
- `pom.xml` - Maven configuration file managing project dependencies and build configurations.

## Dependencies

- **Apache Hadoop:** Version 2.7.1
- **Java 8:** Ensure Java 8 is installed on your system.

# Conclusion

Cheese Analyzer Hadoop is a robust tool for analyzing cheese data, offering powerful filtering and visualization capabilities through an intuitive graphical interface. Whether you're a cheese manufacturer, retailer, or enthusiast, this application provides valuable insights to support informed decision-making.
