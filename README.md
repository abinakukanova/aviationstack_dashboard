# Flight Dashboard over the Black Sea

This project provides a complete pipeline for collecting, storing, and visualizing flight data over the Black Sea region.

## Overview

Using data from OpenSky Network and Aviationstack API, this project gathers information about flights and aircraft models operating over the Black Sea. The collected data is enriched with airport coordinates (from `airports.csv`), stored in a PostgreSQL database, and then visualized on an interactive dashboard built with Dash, Folium, and Plotly.

## Features

- Data Collection: 
  - Flight ICAO codes over the Black Sea are obtained via the OpenSky Network.
  - Detailed flight and aircraft information is retrieved using the Aviationstack API.
  - Geographical coordinates for airports are merged from `airports.csv`.

- Database:
  - All flight-related data is normalized and stored in a PostgreSQL database.
  - An entity-relationship diagram (ERD) is included in the repository to illustrate the database schema.

- Visualization:
  - Flight routes are plotted on an interactive map using Folium.
  - Aircraft model and airline distributions are shown through Plotly bar charts.
  - A full dashboard is built using Dash, allowing filtering of routes by aircraft models.

## Project Structure

```
project/
│
├── data_loader.py        # Data collection and loading into the database
├── dashboard.py          # Dash app and visualizations
├── run_dashboard.py      # Entry point: loads data and starts the dashboard
├── airports.csv          # Airport metadata with coordinates
├── requirements.txt      # Python dependencies
├── erd.png               # Entity-Relationship Diagram of the database
```


## How to Run

1. Clone the repository.
2. Add your Aviationstack API keys and your database name in `run_dashboard.py`.
3. Run `run_dashboard.py` file
4. The dashboard will be available at http://127.0.0.1:8050/ 

## Dependencies
 • Dash
 • Plotly
 • Folium
 • Pandas
 • SQLAlchemy
 • Psycopg2
 • PostgreSQL
 • Requests

## ER Diagram

An ER diagram describing the structure of the PostgreSQL database is included as `erd.png`.
