⚙️ Prerequisites

Before running the pipeline, I made sure that the Databricks environment was properly set up with access to Unity Catalog and an existing workspace.
I created a dedicated catalog called dev, together with four schemas: bronze, silver, gold, and config, to clearly separate the layers and configuration objects.
I also created a Unity Catalog Volume at the path /Volumes/dev/config/configuration, which acts as the central storage for all pipeline assets. Inside this volume, I organized the structure into four folders: bronze, silver, gold, and scripts.
Each layer folder contains CSV schema definition files, where I define column_name, data_type, and comment. These schemas are used by the create_tables.py script to dynamically generate the Delta tables.
For the Gold layer, I added SQL files inside the scripts folder. Each file represents a final Gold table, and the file name directly maps to the table name in the database.
To enable API access, I configured a Databricks secret scope called nlr, with a key named transport_api_key, which is used for external data ingestion.

▶️ Pipeline Execution

The execution is strictly sequential because each layer depends on the previous one.
First, I run create_tables.py, which reads the schema definitions from the volume and creates all empty tables across Bronze, Silver, and Gold layers.
After that, I run bronze.py, which ingests raw data from external APIs and public datasets and stores it in the dev.bronze schema without any transformations.
Next, I run silver.py, where I clean and standardize the data. Here I normalize string values, remove invalid or missing entries like NULL or N/A, deduplicate records, and convert array-like fields into proper array structures. The output is stored in dev.silver.
Finally, I run gold.py, which executes the SQL scripts stored in the volume and builds the final business-ready datasets in dev.gold.

⚠️ Important Notes

The entire pipeline must always be executed in the correct order: create_tables → bronze → silver → gold.
Each layer is dependent on the previous one, meaning Silver reads from Bronze, and Gold reads from Silver. If one step fails, the downstream layers cannot be reliably built.
