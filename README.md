# NTSB-Aviation-Data

This project is a comprehensive pipeline for analyzing and visualizing aviation incidents in the United States using data from the National Transportation Safety Board (NTSB). The project is divided into three main components:

1. **Data Acquisition** (`NTSBquery.py`):
   - Automates the process of querying the NTSB database and downloading aviation incident data in CSV format.
   - Handles data ingestion into a PostgreSQL database, ensuring updates through conflict resolution on unique identifiers.

2. **Data Cleaning and Processing** (`aviationCleaning.py`):
   - Cleans and preprocesses the raw NTSB data to enhance usability for analysis.
   - Performs tasks like missing value imputation, column standardization, data type conversion, and filtering for incidents in the United States.
   - Stores the cleaned data into a separate PostgreSQL table for downstream use.

3. **Data Visualization** (`aviationVisual.py`):
   - A Dash web application providing an interactive map and filters for exploring aviation incidents.
   - Allows users to filter data by month, year, weather conditions, injury levels, engine count, and aircraft category.
   - Displays incidents on a map using `dash-leaflet`, with custom icons for different aircraft types and detailed incident popups.
   - Includes functionality for viewing plotted vs. unplotted incidents, emphasizing data completeness.

### Features
- **Automated Data Workflow**: From querying and cleaning data to visualization, the project automates each step for seamless analysis.
- **Interactive Exploration**: Filter incidents by various attributes and view details directly on a map.
- **Custom Icons**: Different aircraft types are represented by visually distinct markers.
- **PostgreSQL Integration**: Uses a relational database for efficient data storage and querying.

### Technology Stack
- **Programming Languages**: Python
- **Libraries**: Dash, Dash-Leaflet, Pandas, BeautifulSoup, Psycopg2
- **Database**: PostgreSQL
- **Data Source**: [NTSB Aviation Query](https://www.ntsb.gov/Pages/AviationQueryv2.aspx)

### Usage
- Run `NTSBquery.py` to fetch and store raw data.
- Execute `aviationCleaning.py` to process and clean the data for analysis.
- Launch `aviationVisual.py` to explore incidents through the interactive web application.


