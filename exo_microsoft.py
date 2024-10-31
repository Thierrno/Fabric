


######################             Transform data for Silver     ##############################################################################################################

######################
from pyspark.sql.types import *

# Create the schema for the table
orderSchema = StructType([
     StructField("SalesOrderNumber", StringType()),
     StructField("SalesOrderLineNumber", IntegerType()),
     StructField("OrderDate", DateType()),
     StructField("CustomerName", StringType()),
     StructField("Email", StringType()),
     StructField("Item", StringType()),
     StructField("Quantity", IntegerType()),
     StructField("UnitPrice", FloatType()),
     StructField("Tax", FloatType())
     ])

# Import all files from bronze folder of lakehouse
df = spark.read.format("csv").option("header", "true").schema(orderSchema).load("Files/bronze/*.csv")

######################
from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name
    
# Add columns IsFlagged, CreatedTS and ModifiedTS
df = df.withColumn("FileName", input_file_name()) \
     .withColumn("IsFlagged", when(col("OrderDate") < '2019-08-01',True).otherwise(False)) \
     .withColumn("CreatedTS", current_timestamp())\
     .withColumn("ModifiedTS", current_timestamp())
    
# Update CustomerName to "Unknown" if CustomerName null or empty
df = df.withColumn("CustomerName", when((col("CustomerName").isNull() | (col("CustomerName")=="")),lit("Unknown")).otherwise(col("CustomerName")))

######################
 # Define the schema for the sales_silver table
    
from pyspark.sql.types import *
from delta.tables import *
    
DeltaTable.createIfNotExists(spark) \
     .tableName("sales.sales_silver") \
     .addColumn("SalesOrderNumber", StringType()) \
     .addColumn("SalesOrderLineNumber", IntegerType()) \
     .addColumn("OrderDate", DateType()) \
     .addColumn("CustomerName", StringType()) \
     .addColumn("Email", StringType()) \
     .addColumn("Item", StringType()) \
     .addColumn("Quantity", IntegerType()) \
     .addColumn("UnitPrice", FloatType()) \
     .addColumn("Tax", FloatType()) \
     .addColumn("FileName", StringType()) \
     .addColumn("IsFlagged", BooleanType()) \
     .addColumn("CreatedTS", DateType()) \
     .addColumn("ModifiedTS", DateType()) \
     .execute()
######################
# Update existing records and insert new ones based on a condition defined by the columns SalesOrderNumber, OrderDate, CustomerName, and Item.

from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/sales_silver')
    
dfUpdates = df
    
deltaTable.alias('silver') \
  .merge(
    dfUpdates.alias('updates'),
    'silver.SalesOrderNumber = updates.SalesOrderNumber and silver.OrderDate = updates.OrderDate and silver.CustomerName = updates.CustomerName and silver.Item = updates.Item'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "SalesOrderNumber": "updates.SalesOrderNumber",
      "SalesOrderLineNumber": "updates.SalesOrderLineNumber",
      "OrderDate": "updates.OrderDate",
      "CustomerName": "updates.CustomerName",
      "Email": "updates.Email",
      "Item": "updates.Item",
      "Quantity": "updates.Quantity",
      "UnitPrice": "updates.UnitPrice",
      "Tax": "updates.Tax",
      "FileName": "updates.FileName",
      "IsFlagged": "updates.IsFlagged",
      "CreatedTS": "updates.CreatedTS",
      "ModifiedTS": "updates.ModifiedTS"
    }
  ) \
  .execute()
###################### END Transform data for Silver ##################################################################################

######################      Transform data for Gold     ##############################################################################################################
# Load data to the dataframe as a starting point to create the gold layer
df = spark.read.table("Sales.sales_silver")

######################

from pyspark.sql.types import *
from delta.tables import*
    
# Define the schema for the dimdate_gold table
DeltaTable.createIfNotExists(spark) \
     .tableName("sales.dimdate_gold") \
     .addColumn("OrderDate", DateType()) \
     .addColumn("Day", IntegerType()) \
     .addColumn("Month", IntegerType()) \
     .addColumn("Year", IntegerType()) \
     .addColumn("mmmyyyy", StringType()) \
     .addColumn("yyyymm", StringType()) \
     .execute()


######################

from pyspark.sql.functions import col, dayofmonth, month, year, date_format
    
# Create dataframe for dimDate_gold
    
dfdimDate_gold = df.dropDuplicates(["OrderDate"]).select(col("OrderDate"), \
         dayofmonth("OrderDate").alias("Day"), \
         month("OrderDate").alias("Month"), \
         year("OrderDate").alias("Year"), \
         date_format(col("OrderDate"), "MMM-yyyy").alias("mmmyyyy"), \
         date_format(col("OrderDate"), "yyyyMM").alias("yyyymm"), \
     ).orderBy("OrderDate")



###########  Mis à jour avec les nouvelles données pour la table  dfdimDate_gold ###########

from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/dimdate_gold')
    
dfUpdates = dfdimDate_gold
    
deltaTable.alias('gold') \
   .merge(
     dfUpdates.alias('updates'),
     'gold.OrderDate = updates.OrderDate'
   ) \
    .whenMatchedUpdate(set =
     {
          
     }
   ) \
  .whenNotMatchedInsert(values =
     {
       "OrderDate": "updates.OrderDate",
       "Day": "updates.Day",
       "Month": "updates.Month",
       "Year": "updates.Year",
       "mmmyyyy": "updates.mmmyyyy",
       "yyyymm": "yyyymm"
     }
   ) \
   .execute()
######################

from pyspark.sql.types import *
from delta.tables import *
    
# Create customer_gold dimension delta table
DeltaTable.createIfNotExists(spark) \
     .tableName("sales.dimcustomer_gold") \
     .addColumn("CustomerName", StringType()) \
     .addColumn("Email",  StringType()) \
     .addColumn("First", StringType()) \
     .addColumn("Last", StringType()) \
     .addColumn("CustomerID", LongType()) \
     .execute()

############   Ajout de colonne ID ########## 

from pyspark.sql.functions import col, split
    
# Create customer_silver dataframe
    
dfdimCustomer_silver = df.dropDuplicates(["CustomerName","Email"]).select(col("CustomerName"),col("Email")) \
     .withColumn("First",split(col("CustomerName"), " ").getItem(0)) \
     .withColumn("Last",split(col("CustomerName"), " ").getItem(1)) 


######################
from pyspark.sql.functions import monotonically_increasing_id, col, when, coalesce, max, lit
    
dfdimCustomer_temp = spark.read.table("Sales.dimCustomer_gold")
    
MAXCustomerID = dfdimCustomer_temp.select(coalesce(max(col("CustomerID")),lit(0)).alias("MAXCustomerID")).first()[0]
    
dfdimCustomer_gold = dfdimCustomer_silver.join(dfdimCustomer_temp,(dfdimCustomer_silver.CustomerName == dfdimCustomer_temp.CustomerName) & (dfdimCustomer_silver.Email == dfdimCustomer_temp.Email), "left_anti")
    
dfdimCustomer_gold = dfdimCustomer_gold.withColumn("CustomerID",monotonically_increasing_id() + MAXCustomerID + 1)

######################
from delta.tables import *

deltaTable = DeltaTable.forPath(spark, 'Tables/dimcustomer_gold')
    
dfUpdates = dfdimCustomer_gold
    
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.CustomerName = updates.CustomerName AND gold.Email = updates.Email'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "CustomerName": "updates.CustomerName",
      "Email": "updates.Email",
      "First": "updates.First",
      "Last": "updates.Last",
      "CustomerID": "updates.CustomerID"
    }
  ) \
  .execute()


######################

from pyspark.sql.types import *
from delta.tables import *
    
DeltaTable.createIfNotExists(spark) \
    .tableName("sales.dimproduct_gold") \
    .addColumn("ItemName", StringType()) \
    .addColumn("ItemID", LongType()) \
    .addColumn("ItemInfo", StringType()) \
    .execute()


######################
from pyspark.sql.functions import col, split, lit, when
    
# Create product_silver dataframe
    
dfdimProduct_silver = df.dropDuplicates(["Item"]).select(col("Item")) \
    .withColumn("ItemName",split(col("Item"), ", ").getItem(0)) \
    .withColumn("ItemInfo",when((split(col("Item"), ", ").getItem(1).isNull() | (split(col("Item"), ", ").getItem(1)=="")),lit("")).otherwise(split(col("Item"), ", ").getItem(1))) 
######################
from pyspark.sql.functions import monotonically_increasing_id, col, lit, max, coalesce
    
#dfdimProduct_temp = dfdimProduct_silver
dfdimProduct_temp = spark.read.table("Sales.dimProduct_gold")
    
MAXProductID = dfdimProduct_temp.select(coalesce(max(col("ItemID")),lit(0)).alias("MAXItemID")).first()[0]
    
dfdimProduct_gold = dfdimProduct_silver.join(dfdimProduct_temp,(dfdimProduct_silver.ItemName == dfdimProduct_temp.ItemName) & (dfdimProduct_silver.ItemInfo == dfdimProduct_temp.ItemInfo), "left_anti")
    
dfdimProduct_gold = dfdimProduct_gold.withColumn("ItemID",monotonically_increasing_id() + MAXProductID + 1)


######################
from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/dimproduct_gold')
            
dfUpdates = dfdimProduct_gold
            
deltaTable.alias('gold') \
  .merge(
        dfUpdates.alias('updates'),
        'gold.ItemName = updates.ItemName AND gold.ItemInfo = updates.ItemInfo'
        ) \
        .whenMatchedUpdate(set =
        {
               
        }
        ) \
        .whenNotMatchedInsert(values =
         {
          "ItemName": "updates.ItemName",
          "ItemInfo": "updates.ItemInfo",
          "ItemID": "updates.ItemID"
          }
          ) \
          .execute()



######################
from pyspark.sql.types import *
from delta.tables import *
    
DeltaTable.createIfNotExists(spark) \
    .tableName("sales.factsales_gold") \
    .addColumn("CustomerID", LongType()) \
    .addColumn("ItemID", LongType()) \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Quantity", IntegerType()) \
    .addColumn("UnitPrice", FloatType()) \
    .addColumn("Tax", FloatType()) \
    .execute()

######################

from pyspark.sql.functions import col
    
dfdimCustomer_temp = spark.read.table("Sales.dimCustomer_gold")
dfdimProduct_temp = spark.read.table("Sales.dimProduct_gold")
    
df = df.withColumn("ItemName",split(col("Item"), ", ").getItem(0)) \
    .withColumn("ItemInfo",when((split(col("Item"), ", ").getItem(1).isNull() | (split(col("Item"), ", ").getItem(1)=="")),lit("")).otherwise(split(col("Item"), ", ").getItem(1))) \
    
    
# Create Sales_gold dataframe
    
dffactSales_gold = df.alias("df1").join(dfdimCustomer_temp.alias("df2"),(df.CustomerName == dfdimCustomer_temp.CustomerName) & (df.Email == dfdimCustomer_temp.Email), "left") \
        .join(dfdimProduct_temp.alias("df3"),(df.ItemName == dfdimProduct_temp.ItemName) & (df.ItemInfo == dfdimProduct_temp.ItemInfo), "left") \
    .select(col("df2.CustomerID") \
        , col("df3.ItemID") \
        , col("df1.OrderDate") \
        , col("df1.Quantity") \
        , col("df1.UnitPrice") \
        , col("df1.Tax") \
    ).orderBy(col("df1.OrderDate"), col("df2.CustomerID"), col("df3.ItemID"))
######################

from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/factsales_gold')
    
dfUpdates = dffactSales_gold
    
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.OrderDate = updates.OrderDate AND gold.CustomerID = updates.CustomerID AND gold.ItemID = updates.ItemID'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "CustomerID": "updates.CustomerID",
      "ItemID": "updates.ItemID",
      "OrderDate": "updates.OrderDate",
      "Quantity": "updates.Quantity",
      "UnitPrice": "updates.UnitPrice",
      "Tax": "updates.Tax"
    }
  ) \
  .execute()
######################
######################
######################
######################

######################
######################

####################################################################################################################################