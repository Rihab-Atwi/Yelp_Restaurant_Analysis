
<img  src="./readme/title1.svg"/>

<div>

> Hello world! This is the project’s summary that describes the project, plain and simple, limited to the space available.
**[PROJECT PHILOSOPHY](#project-philosophy) • [PROTOTYPING](#prototyping) • [TECH STACKS](#stacks) • [IMPLEMENTATION](#demo) • [HOW TO RUN?](#run)**

</div> 
  

<br><br>

<!-- project philosophy -->

<a  name="philosophy" ></a>
<img  src="./readme/title2.svg" id="project-philosophy"/>

> An ETL project centered on Yelp restaurant data, aiming to extract, transform, and load the data for the purpose of gaining insights into the restaurant industry, encompassing customer preferences, user behavior, and business performance.

<br>

  

### User Types

 

1. Data Engineers.
2. Data Analysts.
3. Restaurant Owners and Managers.
4. Market Researchers.
5. Investors and Entrepreneurs
  

<br>

  

### User Stories

  
1. As Data Engineers:<br>
	I want to design an ETL pipeline that efficiently collects and processes Yelp restaurant data.<br> I want to automate data extraction from the Yelp dataset to keep our dataset current.<br>
	Ensure data quality and integrity in the ETL process, using the filtered Yelp dataset containing only restaurant data.
2. As Data Analysts:
	I want access to the restaurant dataset to perform in-depth analysis.<br>
	I want to generate reports and insights on restaurant trends, customer behavior, and ratings.<br>
	I want the ability to query the database for specific analyses.
3. As Restaurant Owners and Managers:
	I want to understand customer reviews and ratings to improve our restaurant's performance.<br>
	I want to identify areas for improvement and assess our restaurant's online reputation.<br>
	Access to data visualizations for quick insights.
4. As Market Researchers:
	I want to analyze the restaurant industry's competitive landscape.<br>
	I want to track consumer preferences and dining trends.<br>
	Access to comprehensive restaurant data for market research.
5. As Investors and Entrepreneurs:
	I want insights into the restaurant industry to identify investment opportunities.<br>
	I want to evaluate the performance and growth potential of different restaurants.<br>
	Access to data that can inform investment decisions in the food service sector.

<br><br>

<!-- Prototyping -->
<img  src="./readme/title3.svg"  id="prototyping"/>

> We have designed our projects to webscrape, through an ETL project and including it in a PowerBI Sample Dashboard, 

  

### Logger File

  

| Bins Map screen | Dashboard screen | Bin Management screen |

| ---| ---| ---|

| ![Landing](./readme/wireframes/web/map.png) | ![Admin Dashboard](./readme/wireframes/web/dashboard.png) | ![User Management](./readme/wireframes/web/bin_crud.png) |

  
  

### Data Flow Diagrams

Database Schema
<img src = "./readme/db_schema (1).jpg" />
<br>
Powerbi Home Page:
<br>
<img src = "./readme/powerbi_sc/Final Project.PNG" />
<br>
Overview:
<br>
<img src = "./readme/powerbi_sc/Capture.PNG" />
<br>
Snapshot of Restaurants:
<br>
<img src = "./readme/powerbi_sc/snapshot.PNG" />
<br>
Top-Bottom Restaurants:
<br>
<img src = "./readme/powerbi_sc/top-bottom.PNG" />
<br>
Restaurants Reviews:
<br>
<img src = "./readme/powerbi_sc/Reviews.PNG" />
<br>
Users:
<br>
<img src = "./readme/powerbi_sc/users.PNG" />




<!-- Tech stacks -->

<a  name="stacks"></a>
<img  src="./readme/title4.svg" id="stacks" />

<br>

  

Bin Tracker is built using the following technologies:

  

## Frontend

Interactive PowerBI Dashboard:<br>
A central dashboard where viewers can view:

1. Restaurant Insights: Visualizations including graphs, charts, and data representations showcasing key restaurant attributes and trends over time.
2. Review Sentiment Analysis: I conducted sentiment analysis to understand public sentiment about the restaurants, presenting the results through pie charts and sentiment bars. Additionally, I performed topic modeling to identify the most frequently occurring topics in both positive and negative reviews.
3. Comprehensive Analysis of Yelp Restaurants: Unveiling Common Attributes,Categories, buseist hour and day.
4. Interactive filters: options to filter by region.


  

<br>

  

## Backend

1. Extract yelp data.
2. ETL Pipeline: using python and pandas, raw data is extracted, transformed into a usable format and loaded into postgreSQL database.
3. Database: Schema Design - Indexing - Data Integrity - Backup & Recovery.
4. Machine Learning: Sentiment Analysis - Topic Modeling.  

<br>

<br>

  

<!-- Implementation -->

<a  name="Demo"  ></a>
<img  src="./readme/title5.svg" id="#demo"/>

> Show command line of ETL performance - Logger view

  
### App

<br><br>

### Machine Learning (ML) component
In my Yelp restaurant analysis project, I collected and preprocessed Yelp restaurant reviews. For topic modeling, I used Latent Dirichlet Allocation (LDA) without training a custom model. Sentiment analysis was done using the VADER tool from NLTK without model training.

I evaluated these pre-trained models based on their performance in uncovering topics and assessing sentiment in the reviews. 
  

<!-- How to run -->

<a  name="run"  ></a>
<img  src="./readme/title6.svg" id="run"/>
  

> To set up ## **Yelp Restaurant Analysis** follow these steps:

### Prerequisites


**Hardware & Software**:

-   A computer/server with sufficient RAM and processing power.
-   Operating system: Windows.
-   Required software: Python (3.x), PostgreSQL, Git (for version control), and essential software packages including vaderSentiment, nltk, scikit-learn, and langdetect to collect, preprocess, and analyze Yelp reviews for sentiment, topics, and language,
  
  

**Dependencies**:

-   Install the necessary Python libraries: `pandas`, `psycopg2-binary`, `scikit-learn`, `vaderSentiment` `nltk`, and `langdetect`.
-   Install database connectors/drivers for PostgreSQL.
  

### **Setting Up the Environment**:

**Clone the Repository**:


```sh

git clone https://github.com/Rihab-Atwi/final_project_yelp.git

```

  
**Set Up the Database**:

-   Start the PostgreSQL server.
-   Create a new database and user with the appropriate permissions.
-   Run any initialization scripts to set up tables or initial.

### **Running the Backend**:

**Start the Data Ingestion & ETL Process**:
`python data_ingestion_script.py`


You should be able to check the app.log file to see the ETL work.

As for the dashboard access: Please use this link "public powerbi link" to access your data.
