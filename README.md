# Export Market Trends Dashboard for Commodities in Indonesia

## 📖 Overview

The **Export Market Trends Dashboard for Commodities in Indonesia** is a data-driven solution designed to analyze and predict commodity market trends. By leveraging predictive analytics, clustering, and macroeconomic visualizations, the dashboard helps stakeholders like policymakers, businesses, and market participants make strategic decisions to mitigate risks and ensure economic stability. 

---

## 📊 Key Features

1. **Commodities Risk Assessment**  
   - Uses clustering (K-Means) and unsupervised learning to classify commodities into high-risk or low-risk categories.  
   - Enables stakeholders to understand associated risks and plan accordingly.

2. **Commodities Price Prediction**  
   - Implements a SARIMAX time series model to forecast commodity prices for the next 12 months with high accuracy.  
   - Supports proactive risk mitigation and strategic planning.

3. **Macroeconomics Data Visualization**  
   - Provides visual insights into key macroeconomic data such as BI Rate, IDR-USD exchange rates, and inflation levels.  
   - Offers actionable insights for data-driven decision-making.  

4. **Demand & Price Visualization**  
   - Year-to-date historical data on commodity demand and prices is visualized interactively.  
   - Helps identify market trends and anticipate future needs.

---

## 🎯 Objectives

- Understand price fluctuation patterns for key commodities like CPO, gold, and coffee.
- Accurately forecast future commodity prices using time series algorithms.
- Classify commodities by risk level for effective risk mitigation.
- Support decision-making with relevant macroeconomic data.
- Enable stakeholders to manage risks associated with price volatility.
- Enhance strategic readiness against global economic changes.

---

## 🚨 Problem Statement

- Global commodity price fluctuations significantly impact Indonesia’s economy, especially in sensitive sectors like energy, agriculture, and metals.
- Price volatility of major commodities such as CPO, gold, and coffee can reach 10-15%, influencing inflation by 4.8% and impacting Indonesia's GDP by 24.5%.
- There is a critical need for tools to monitor risks, predict demands, and reduce the adverse effects of commodity price instability on the national economy.

---

## 💡 Proposed Solution

The solution integrates advanced analytics, predictive modeling, and data visualization to address challenges in commodity markets. Key components include:

1. **Data Sources**  
   - **Badan Pusat Statistik (BPS)**: Provides APIs for BI Rate, inflation, and other macroeconomic data.  
   - **Yahoo Finance**: Supplies exchange rate data (IDR-USD) and commodity price/volume data.  

2. **Predictive Modeling**  
   - **SARIMAX Model**: Forecasts commodity prices for the upcoming 12 months using seasonal and trend-based analysis.  
   - **K-Means Clustering**: Classifies commodities into low-risk and high-risk groups based on historical data.  

3. **Dashboard Infrastructure**  
   - **Database (PostgreSQL)**: Stores processed data for efficient retrieval.  
   - **Visualization (Tableau)**: Presents interactive dashboards with key insights on trends, risks, and macroeconomic factors.  
   - **Data Processing (PySpark)**: Handles large-scale data transformations and predictions efficiently.  

---

## 🛠️ Architecture Overview

The architecture ensures a seamless flow of data from extraction to visualization:

### 1. **Data Sources**
- **BPS API**: Provides macroeconomic data like BI Rate and inflation.  
- **Yahoo Finance API**: Supplies real-time commodity price and volume data.

### 2. **Infrastructure Systems**
- **Apache Airflow**: Automates data extraction, transformation, and loading (ETL).  
- **PySpark**: Processes and transforms large datasets, performs clustering, and applies predictive models.

### 3. **End Systems**
- **PostgreSQL**: Stores structured data for analysis and visualization.  
- **Tableau**: Creates interactive dashboards for users to explore trends and insights.

### Key Advantages
- **Automation**: Apache Airflow minimizes manual intervention in data workflows.  
- **Scalability**: PySpark ensures high-performance processing of large datasets.  
- **User-Friendly Visualizations**: Tableau simplifies complex data into actionable insights.

---

## 🧩 Database Schema

The database is designed using **Second Normal Form (2NF)** to minimize redundancy and ensure efficient data management. Key tables include:

1. **`bi_rate`**: Stores BI Rate data.  
2. **`inflasi_rate`**: Stores inflation data.  
3. **`yf_rate`**: Stores historical data on commodity prices and volumes.  

---

## 🔧 Implementation Details

### 1. **ETL Process**
- **Airflow DAG**: Automates data scraping (commodity prices, BI Rate, inflation), transformation, and loading into the database.

### 2. **Predictive Modelling**
- **SARIMAX**: Used for forecasting commodity prices for 12 months.  
- **K-Means Clustering**: Groups commodities into high-risk and low-risk categories.

### 3. **Transformation**
- Converts raw data into usable formats using PySpark and Python.  
- Maps Indonesian month names to English for compatibility.  
- Handles missing or inconsistent data by standardizing values.

---
