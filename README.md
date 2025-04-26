# 🌦️ Weather Data Pipeline with Airflow, Docker, Postgres & Metabase

This is an end-to-end data pipeline that fetches daily weather data for UK cities using the OpenWeatherMap API, stores it in a PostgreSQL database via Apache Airflow, and visualizes it using Metabase.

---

## 🛠️ Tech Stack

- **Airflow** (ETL Orchestration)
- **PostgreSQL** (Data Warehouse)
- **Docker Compose** (Containerized Infra)
- **Metabase** (Visualization)
- **pgAdmin** (DB GUI)

---

## 🧱 Architecture Overview


---

### 🚀 Features

- Fetches real-time weather data for major UK cities using lat/lon
- Inserts weather data into a normalized Postgres schema
- Automates and schedules via Airflow
- Easy setup using Docker Compose
- Clean dashboard via Metabase

---

### Visual architecture

<p align="center">
  <img src="images/project_architecture_diagram.png" alt="project architecture diagram" width="75%" height="75%">
</p>

---

## 🧪 Setup Instructions

1. **Clone the repository**
   ```bash
   git clone https://github.com/chiranjeevi-sagi/weather-data-pipeline.git
   cd weather-data-pipeline

2. **Create your .env file**
   ```bash
   cp .env.example .env

Then open .env and update the passwords of your choice and replace with your API key.

Sign in/Create account on open weather website and copy your API key. You can get your API key from  [here](https://openweathermap.org/current). 

3. **Start the services**
   ```bash
   docker compose up --build

4. **Access the tools**
- **Airflow:** http://localhost:8080
- **pgAdmin:** http://localhost:5050
- **Metabase:** http://localhost:3000

For logging in, use your credentials set in your .env file.

Run your DAGs using Airflow, Query your Database using pgAdmin and Visualize your data using Metabase.

Want to checkout a demo of this project? go to : https://youtu.be/w9Ke-BMettc

Congratulations on completing your ETL project!

## Credits
Built by [Chiranjeevi Sagi](https://github.com/chiranjeevi-sagi)