
# Se connecter à une source externe

# Azure Blob Storage access info
blob_account_name = "<your_blob_account_name>"
blob_container_name = "<your_blob_container_name>"
blob_relative_path = "<your_blob_relative_path>"

# Construct connection path
abfss_path = f'abfss://{blob_container_name}@{blob_account_name}.dfs.core.windows.net/{blob_relative_path}'

# Read data from Azure Blob Storage path into a DataFrame
blob_df = spark.read.parquet(abfss_path)

# Show the DataFrame
blob_df.show()




# Configurer une autre authentification

# Placeholders for Azure SQL Database connection info
server_name = "<your_server_name>.database.windows.net"
database_name = "<your_database_name>"
table_name = "<YourTableName>"
db_username = "<username>"
db_password = "<password>"

# Build the Azure SQL Database JDBC URL
jdbc_url = f"jdbc:sqlserver://{server_name}:1433;database={database_name};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# Properties for the JDBC connection
properties = {
    "user": db_username, 
    "password": db_password,  
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# Read entire table from Azure SQL Database using Basic authentication
sql_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

# Show the Azure SQL DataFrame
sql_df.show()