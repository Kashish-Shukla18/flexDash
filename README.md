# flexDash

flexDash is a Django-based flexible dashboard application that allows users to interactively visualize and manage data. It supports pulling data from an existing PostgreSQL database, uploading CSV/Excel files, visualizing the data with dynamic charts, and even saving uploaded data back to the database as new tables.

## Features

- **Database Integration**: Connects to a PostgreSQL database. Users can list existing public tables and load data from them directly into the dashboard.
- **File Uploads**: Supports uploading `.csv`, `.xls`, and `.xlsx` files using `pandas`.
- **Dynamic Visualization**: Generates Chart.js-compatible datasets. Users can select X and Y columns and visualize data using various chart types (Bar, Line, Area, Pie, Doughnut, Polar Area).
- **Time-Range Filtering**: Allows filtering data by time ranges if a time column is present in the dataset.
- **Session Caching**: Temporarily caches uploaded or loaded data in server memory (local-memory cache) for 30 minutes to ensure fast interactions and chart rendering without repeatedly hitting the database or filesystem.
- **Data Export to DB**: Allows users to save their uploaded CSV/Excel data as a new table directly into the connected PostgreSQL database.

## Prerequisites

- Python 3.8+
- PostgreSQL database
- Environment variables configured (see Setup)

## Setup and Installation

1. **Clone the repository** (if applicable) or navigate to the project directory:
   ```bash
   cd flexDash
   ```

2. **Create and activate a virtual environment**:
   ```bash
   python -m venv venv
   # On Windows:
   venv\Scripts\activate
   # On macOS/Linux:
   source venv/bin/activate
   ```

3. **Install dependencies**:
   Make sure to install the required Python packages (such as `django`, `pandas`, `psycopg2`, `python-dotenv`, `openpyxl`). If a `requirements.txt` is available, run:
   ```bash
   pip install -r requirements.txt
   ```
   *(If not available, you can install them manually: `pip install django pandas psycopg2-binary python-dotenv openpyxl`)*

4. **Environment Variables**:
   Create a `.env` file in the root directory (alongside `manage.py`) based on the `example.env` file. You need to configure your PostgreSQL credentials:
   ```env
   DB_NAME=your_db_name
   DB_USER=your_db_user
   DB_PASSWORD=your_db_password
   DB_HOST=localhost
   DB_PORT=5432
   ```

5. **Apply Migrations**:
   Run Django migrations to set up the default tables (like sessions, auth, etc.):
   ```bash
   python manage.py migrate
   ```

6. **Run the Development Server**:
   ```bash
   python manage.py runserver
   ```
   Visit `http://127.0.0.1:8000` in your browser to access the dashboard.

## How it Works

1. **Data Ingestion**: Users can either upload a spreadsheet or select a table from the Postgres database. The backend reads this data into a `pandas` DataFrame.
2. **Caching**: The DataFrame is converted to JSON and stored in Django's local memory cache with a session-specific key. This cache expires after 30 minutes by default.
3. **Visualization**: When a user requests a chart, the backend retrieves the dataframe from the cache, applies any requested time filters, and formats the data for Chart.js.
4. **Data Persistence**: If a user uploaded a file and wants to keep it in the database, the backend dynamically constructs a `CREATE TABLE` query and inserts the dataframe's rows into PostgreSQL.

## Technologies Used

- **Backend**: Django, Python
- **Data Manipulation**: Pandas
- **Database**: PostgreSQL
- **Frontend Visualization**: Chart.js (via JSON payloads)
