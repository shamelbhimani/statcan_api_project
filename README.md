# StatCan API Project

This repository contains a Python project that fetches data from the 
Statistics Canada API, processes it, and stores it in a MySQL database. This 
project serves as an 'Extract, Transform, Load' project for my resume, as 
well as a proof-of-concept for a future API-wrapper project that simplifies 
the process of extracting data from Statistics Canada for academic analysis.


## Features

* **API Integration**: Directly connects to the Statistics Canada web data 
  service to fetch data based on specified vectors and time periods.
* **Data Extraction**: Parses the JSON response from the API to extract 
  relevant data points, including vector IDs, product IDs, reference periods,
  and values and processes it into a nested dictionary format.
* **Database Management**: Automatically creates tables in a MySQL database 
  corresponding to product IDs, adds columns for new dates, and inserts or 
  updates data for each vector.
* **Configuration-driven**: Utilizes configuration files for easy management 
  of API endpoints, database credentials, and file paths.



## Setup and Usage

### Prerequisites

* Python 3.12
* MySQL Server
* Required Python packages (installable via pip):
    * `mysql-connector-python == 9.4.0`
    * `requests >= 2.23.5`

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/shamelbhimani/statcan_api_project.git
    cd statcan_api_project
    ```
2.  **Create and configure `secrets.ini`:**
    In the `config/` directory, create a file named `secrets.ini` with your MySQL database credentials:
    ```ini
    [mysql]
    host = YOUR_HOST_HERE
    user = YOUR_USER_HERE
    password = YOUR_PASSWORD_HERE
    database = YOUR_DATABASE_HERE
    ```

### Running the Application

To run the project, execute the `main.py` script from the root directory:

```bash
python main.py
```
You will be asked to input the number of months of data you want to extract. 
Enter the number of months of data you want and the process will execute.

## Project Structure

The repository is organized into the following directories and files:

* `main.py`: The entry point of the application. It orchestrates the process 
of fetching data from the API and updating the database.
* `src/`: Contains the core source code for the project.
    * `api_client.py`: A client to handle interactions with the Statistics 
    Canada API.
    * `database_manager.py`: Manages the MySQL database, including creating 
    tables, and inserting and updating data.
    * `definitions_fetcher.py`: Fetches definitions for tables and vectors 
    from CSV files.
* `config/`: Contains configuration files.
    * `config.ini`: Main configuration file for file paths.
    * `secrets.ini`: For storing database credentials (Note: This file is 
    gitignored).
* `info/`: Contains data files used by the application.
    * `vectors.txt`: A list of vector IDs to fetch data for.
    * `vector_definitions.csv`: Definitions for each vector ID.
    * `table_definitions.csv`: Definitions for each product ID (table).

***
