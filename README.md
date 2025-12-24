# Data Engineering Tutorial: From Raw Data to Azure Synapse Analytics

# Introduction

This guide walks you through creating a scalable data pipeline in Azure, transforming raw data into meaningful insights using Databricks, Azure Data Factory (ADF), and Synapse Analytics.

## **What You’ll Learn**
  1. Configure Azure Databricks and securely access data in Azure Storage.
  2. Process and transform data using Databricks notebooks (`bronze`, `silver`, `gold`).
  3. Automate data pipelines with Azure Data Factory.
  4. Query and optimize data in Synapse Analytics for analytics and visualization.

## **Technologies Used**
 - Azure Databricks
 - Azure Data Factory
 - Azure Synapse Analytics
   
---
<br/>

# Steps

## 1) Create Resource - Databricks
  - Resource Group: `rg-earthquake` (Create New)
  - Workspace name: `earthquake-db`
  - Region: `East US`
  - Click `Create`
<br/>

## 2) Create Resource - Storage Account (ADLS Gen2)
  - Resource Group: `rg-earthquake`
  - Storage account name: `storeearthquake`
  - Region: `East US`
  - Primary service: `Azure Blob Storage or Azure Data Lake Storage Gen 2`
  - Redundancy: `Locally-redundant strage (LRS)` (Cheapest)
  - Click `Create`

  ### Create Storage Account Containers
   - **Data Storage** → **Containers**
   - Create 3 Containers (Bronze, Silver, Gold)
<br/>

## 3) Create Resource - Synapse workspace
  - Resource Group: `rg-earthquake`
  - Workspace name: `earthquake-synapse`
  - Region: `East US`
  - Select Data Lake Storage Gen 2:
    - Account name: `storeearthquake`
    - File system name: `synapse-fs` (New)
    - [X] Assign myself the Storage Bloc Data Contributor on ADLS Gen2
  - Click `Create`
<br/>

## 4) Databricks Deployment
  - Enter Databricks workspace `earthquake-db`
  - What each Tab does:
    - `Workspace`: Store/Create Notebooks
    - `Catalog`:   Connect ADLS storage to be maniupulate
    - `Compute`:   Notebooks run here
   ### Create Compute instance
   1. Click create
   2. Policy: `Unrestricted` - `Single Node`
   3. Access mode: `Single user`
   4. Performance
     - [ ] Use Photon Acceleration (Not needed)
     - Node type: `General purpose` lowest (ie 14 GB Memory, 4 Cores)
     - [X] Terminate after 20 minutes
   5. Click `Create compute`
<br/>

## 5) Setup Secure Connection(ADB <==> ADLS)
  ### Create a Credential (to be used for an external location)
  1. `Catalog` → `External Data` → `Credential` → `Create credential`
  2. Credential type: `Azure Managed Identity`
  3. Credential name: `earthqual-cred`
  4. Access connector ID: `/subscriptions/ca8b577e-..accessConnectors/unity-catalog-access-connector` (**FOUND BELOW**)
     - Azure portal → `rg-earthquake`(resource group) → `earthquake-db` (db resource) → Managed Resource Group: `databricks-rg-earthquake-<unique>` → `unity-catalog-access-connector`
     - COPY Resource ID : `/subscriptions/ca8b577e-..accessConnectors/unity-catalog-access-connector`

  ### Create External Locations
  1. `Catalog` → `External Data` → `Create external location`
  2. Create 3 External Locations for all Medallion Stages
     1. Bronze
        1. External location name: `bronze`
        2. External location name: `abfss://bronze@storeearthquake.dfs.core.windows.net/` (**endpoint to ADLS container**)
        3. Storage Credential: `earthqual-cred` (from 5.3)
     2. Silver
        1. External location name: `silver`
        2. External location name: `abfss://silver@storeearthquake.dfs.core.windows.net/` (**endpoint to ADLS container**)
        3. Storage Credential: `earthqual-cred` (from 5.3)
     3. Gold
        1. External location name: `gold`
        2. External location name: `abfss://gold@storeearthquake.dfs.core.windows.net/` (**endpoint to ADLS container**)
        3. Storage Credential: `earthqual-cred` (from 5.3)
      
  ![](./Drawio.png)
<br/>


<br/>


<br/>


<br/>


<br/>




![Data Engineering vs Software Engineering (6)](https://github.com/user-attachments/assets/bdadd2e0-89be-4683-b53b-fe331be6f6bf)


