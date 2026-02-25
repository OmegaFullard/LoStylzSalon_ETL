# LoStylzSalon ETL

<img width="150" height="150" alt="LogoLoStylz" src="https://github.com/user-attachments/assets/798abd66-6c2e-4663-a9f9-b80bb1179f2d" />


## Project Overview

<img width="150" height="150" alt="ladybg" src="https://github.com/user-attachments/assets/e3969c4b-1ab5-459a-9abb-fe3c1e878de2" />

LoStylzSalon ETL is a comprehensive data processing tool that efficiently handles employee data, customer information, and salon appointment data. Designed to transform CSV files into actionable analytics and reports, this project utilizes Python and SQL for extraction, transformation, and loading (ETL) processes.

## Features
<img width="250" height="200" alt="ETLProcess" src="https://github.com/user-attachments/assets/a0edb30b-dc0b-41a1-964a-85f59a1a8612" />


- **Data Extraction**: Imports employee, customer, and appointment information from CSV files.
- **Data Transformation**: Cleans and transforms data for analytics.
- **Report Generation**: Produces insightful analytics and reports for salon management.

## Architecture
The ETL process consists of three main stages: extraction, transformation, and loading:
1. **Extraction**: Data is collected from CSV files.
2. **Transformation**: Data is processed and refined for better analysis.
3. **Loading**: Cleaned data is loaded into the SQL database for reporting.

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/OmegaFullard/LoStylzSalon_ETL.git
   ```
2. Navigate to the project directory:
   ```bash
   cd LoStylzSalon_ETL
   ```
3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage
1. Place your CSV files in the `data/` directory.
2. Run the ETL process:
   ```bash
   python etl_process.py
   ```
3. Access the generated analytics and reports in the `reports/` directory.

## Project Structure
```
LoStylzSalon_ETL/
├── data/
│   ├── employees.csv
│   ├── customers.csv
│   └── appointments.csv
├── reports/
│   └── analytics_reports.csv
├── etl_process.py
└── requirements.txt
```

## Data Sources
- **Employee Data**: Contains details like name, position, and salary.
- **Customer Information**: Includes customer names, contact details, and appointment histories.
- **Salon Appointment Data**: Records appointments, including timestamps and services rendered.

## Transformations
- Data cleaning: Handle missing values and duplicates.
- Data normalization: Format and standardize data entries.
- Aggregation: Calculate metrics such as total appointments and employee performance.

## Prerequisites
- Python 3.6 or higher
- SQL database (e.g., SQLite, MySQL)

## Contributing Guidelines
1. Fork the repository.
2. Create a new branch for your feature/bug fix:
   ```bash
   git checkout -b feature/new-feature
   ```
3. Commit your changes:
   ```bash
   git commit -m "Add new feature"
   ```
4. Push to the branch:
   ```bash
   git push origin feature/new-feature
   ```
5. Open a Pull Request.
