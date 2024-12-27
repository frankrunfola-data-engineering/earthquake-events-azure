# Earthquake Azure Data Engineering Pipeline: A Comprehensive Guide

# Chapter 1: Overview and Architecture

### Business Case

Earthquake data is incredibly valuable for understanding seismic events and mitigating risks. Government agencies, research institutions, and insurance companies rely on up-to-date information to plan emergency responses and assess risks. With this automated pipeline, we ensure these stakeholders get the latest data in a way that’s easy to understand and ready to use, saving time and improving decision-making.

### Architecture Overview

This pipeline follows a modular architecture, integrating Azure’s powerful data engineering tools to ensure scalability, reliability, and efficiency. The architecture includes:

1. **Data Ingestion**: Azure Data Factory orchestrates the daily ingestion of earthquake data from the USGS Earthquake API.
2. **Data Processing**: Databricks processes raw data into structured formats (bronze, silver, gold tiers).
3. **Data Storage**: Azure Data Lake Storage serves as the backbone for storing and managing data at different stages.
4. **Data Analysis**: Synapse Analytics enables querying and aggregating data for reporting.
5. **Optional Visualization**: Power BI can be used to create interactive dashboards for stakeholders.

### Data Modeling

We implement a **medallion architecture** to structure and organize data effectively:

1. **Bronze Layer**: Raw data ingested directly from the API, stored in Parquet format for future reprocessing if needed.
2. **Silver Layer**: Cleaned and normalized data, removing duplicates and handling missing values, ensuring it’s ready for analytics.
3. **Gold Layer**: Aggregated and enriched data tailored to specific business needs, such as calculating earthquake severity counts by region.

### Understanding the API

- The earthquake API provides detailed seismic event data for a specified start and end date.
- **Start Date**: Defines the range of data. This is dynamically set via Azure Data Factory for daily ingestion.
- **API URL**: `https://earthquake.usgs.gov/fdsnws/event/1/`

### Key Benefits

- **Automation**: Eliminates manual data fetching and processing, reducing operational overhead.
- **Scalability**: Handles large volumes of data seamlessly using Azure services.
- **Actionable Insights**: Provides stakeholders with ready-to-use data for informed decision-making.

---

## Chapter 2: Setting Up Your Environment

1. **Create an Azure Account**: If you don’t already have one, sign up for an Azure account.
2. **Provision a Databricks Resource**:
    - Use the trial version for testing (takes about 5 minutes).
    - Select the **Standard LTS (Long Term Support)** edition. Avoid options like ML.
3. **Create a Storage Account**:
    - Enable **Hierarchical Namespaces** in the advanced settings to support future data mounting.
    - Go to the Storage Account resource:
        1. Navigate to **Data Storage > Containers > + Containers**.
        2. Create containers named **bronze**, **silver**, and **gold**.
        3. Go to **IAM > Add Role Assignment**:
            - Role: **Storage Blob Data Contributor**.
            - Managed Identity: **Access Connector for Azure Databricks**.
            - Assign the role and complete the setup.
4. **Set Up Synapse Analytics Workspace**:
    - Navigate to the Azure Portal.
    - Search for **Synapse Analytics** and click **+ Create**.
    - Provide the required details:
        - Workspace name: `MySynapseWorkspace`.
        - Use the previously created Storage Account (Data Lake Storage Gen2).
        - Create a new file system and assign yourself as a contributor.
    - Review and create the workspace.

---

## Chapter 3: Configuring Databricks

1. **Launch the Databricks Workspace**:
    - Start a compute instance (this may take a few minutes).
2. **Set Up External Data in Databricks**:
    - Go to **Catalog > External Data > Credentials > Create Credential**.
    - Use the **Resource ID** of the Access Connector:
        - Search for the Access Connector in Azure.
        - Copy the Resource ID.
    - Grant permissions (optional: use the three dots menu for managing permissions).
3. **Create External Locations**:
    - Go to **External Data > External Locations**.
    - Add a name, select the new storage credential, and input the URL for each container (**bronze**, **silver**, **gold**).
4. **Create and Test Notebooks**:
    - Navigate to **Workspace > Create Notebook**.
    - Write and run code for the **bronze** container.
    - Refresh the Storage Account containers to verify updates.
    - Repeat for **silver** and **gold** notebooks.

---

## Chapter 4: Optimizing Libraries and Performance

1. **Install Libraries for Notebooks**:
    - Use cluster-level libraries to ensure consistency across environments.
    - Go to **Compute > Your Cluster > Libraries**.
    - Source: **PyPI**, Name: `reverse_geocoder`.
2. **Resolve Performance Issues**:
    - Databricks may expose inefficiencies in UDFs due to its distributed execution.
    - Replace Python UDFs with:
        - **Precomputed lookup tables**.
        - **Pandas UDFs** for vectorized execution.
        - **Batch processing geocoding** outside of Spark.

---

## Chapter 5: Integrating Azure Data Factory (ADF)

1. **Create an Azure Data Factory Resource**:
    - Launch the ADF Studio.
    - Create a new pipeline and add a Databricks notebook activity.
2. **Set Up Linked Services**:
    - Create a Databricks Linked Service:
        - Select your subscription and workspace.
        - Use an **Access Token** for authentication (generate in Databricks).
    - Optionally, store the token in an Azure Key Vault for security.
        - Assign roles like **Key Vault Secret Officer** and **Key Vault Secret User**.
        - Create a Linked Service from ADF to the Key Vault.
3. **Configure and Schedule Pipelines**:
    - Define pipeline parameters such as `start_date` and `end_date`.
    - Link the **bronze**, **silver**, and **gold** notebooks in sequence.
    - Validate and publish the pipeline.
    - Schedule triggers for automated runs.

---

## Chapter 6: Querying and Analyzing Data in Synapse Analytics

1. **Set Up SQL Pools and Serverless SQL**:
    - Open Synapse Studio.
    - Create linked services for your Data Lake Storage.
2. **Query Data Using Serverless SQL**:
    - Navigate to the `bronze`, `silver`, and `gold` containers.
    - Use SQL scripts to query the data, e.g.,
        
        ```
        SELECT
            country_code,
            COUNT(CASE WHEN LOWER(sig_class) = 'low' THEN 1 END) AS low_count,
            COUNT(CASE WHEN LOWER(sig_class) IN ('medium', 'moderate') THEN 1 END) AS medium_count,
            COUNT(CASE WHEN LOWER(sig_class) = 'high' THEN 1 END) AS high_count
        FROM
            OPENROWSET(
                BULK 'https://storageaccount.dfs.core.windows.net/gold/earthquake_events_gold/**',
                FORMAT = 'PARQUET'
            ) AS [result]
        GROUP BY
            country_code;
        ```
        
3. **Optimize Performance**:
    - Use indexing, partitioning, and caching for faster queries.

---

## Chapter 7: Optional Integration with Power BI

1. **Connect Power BI to Synapse**:
    - Open Power BI Desktop and select **Get Data > Azure Synapse Analytics**.
    - Use the Synapse SQL endpoint for querying.
2. **Visualize Data**:
    - Import external tables or queries into Power BI.
    - Create dashboards and reports for insights.

---

## Conclusion

This guide provides a step-by-step approach to building a robust Azure data engineering pipeline. By integrating Databricks, Synapse Analytics, and ADF, you can process, analyze, and visualize data efficiently, ensuring best practices and scalability.