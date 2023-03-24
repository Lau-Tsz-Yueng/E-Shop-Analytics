# E-Shop-Analytics

This project analyzes the sales data of an e-commerce store and provides useful insights on customer behavior, order values, discounts, and commission rates.

## Overview

The project consists of a set of Python scripts and a Flask API that provide the following functionalities:

1. Load and preprocess sales data from CSV files
2. Analyze the data to calculate various statistics like the number of unique customers, average order value, average discount rate, etc.
3. A Flask API to query the data and return the calculated statistics

## Getting Started

### Prerequisites

- Python 3.6 or later
- pandas
- Flask

### Installation

1. Clone this repository:

```
git clone https://github.com/Lau-Tsz-Yueng/E-Shop-Analytics.git
```

2. Change to the project directory:

```
cd E-Shop-Analytics
```

3. Install the required packages:

```
pip install -r requirements.txt
```

## Usage

### Running the Flask API

1. Run the Flask API by executing:

```
python app.py
```

2. Access the API at `http://127.0.0.1:5000/api/statistics/YYYY-MM-DD`, replacing `YYYY-MM-DD` with the desired date. (Please select a date between August 1, 2019 and September 29, 2019)

### Example

To get sales statistics for September 1, 2019, visit the following URL in your browser or use a tool like `curl`:

```
http://127.0.0.1:5000/api/statistics/2019-09-01
```

The API will return the data in JSON format.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
