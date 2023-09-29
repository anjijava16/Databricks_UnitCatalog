// Databricks notebook source
// MAGIC %md
// MAGIC #Connecting Azure Databricks to Cassandra
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Establish network paths
// MAGIC ### Options
// MAGIC 1. Peer the private vnet as per [VNet Peering](https://docs.azuredatabricks.net/administration-guide/cloud-configurations/azure/vnet-peering.html#vnet-peering)
// MAGIC   1. In the azure portal under the databricks workspace asset, choose peering blade
// MAGIC   1. Peer the VNet where your Cassandra vms are deployed (You don't need transit routing and such--just a vanilla IP space peering suffices)
// MAGIC   1. In the VNet where your Cassandra vms are deployed, peer the locked VNet where databricks is working
// MAGIC 1. Use VNet injection as per [VNet Injection](https://docs.azuredatabricks.net/administration-guide/cloud-configurations/azure/vnet-inject.html)
// MAGIC   1. Follow steps for VNet injection
// MAGIC   1. Use json template provided to deploy workspace using your configured VNet
// MAGIC 1. Make Cassandra available over public IP paths 
// MAGIC   * Likely will need to do ACL and Firewall work with heaving auditing

// COMMAND ----------

// MAGIC %md
// MAGIC ##Verify network connectivity to the Cassandra node
// MAGIC   * Working with a single node Cassandra cluster with IP 172.16.1.4 for this example

// COMMAND ----------

// MAGIC %sh
// MAGIC echo "Success will show 5 packets sent with 0 packet loss"
// MAGIC ping -c 5 172.16.1.4
// MAGIC echo "Look for 0 packet loss"
// MAGIC echo "You should see: (UNKNOWN) [IP] PORT (?) open"
// MAGIC nc -nzv 172.16.1.4 9042
// MAGIC echo ""

// COMMAND ----------

// MAGIC %md
// MAGIC ##Load the connector library 
// MAGIC   * only needs to happen once per workspace unless you need different clusters on different versions
// MAGIC   
// MAGIC 1. In the workspace somewhere choose create->library (e.g., Go to Workspace->Shared->CassandraWork then hit the options inverted carot, then create->library)
// MAGIC 1. On the page that comes up
// MAGIC   1. Choose source: maven coordinates
// MAGIC   1. Hit the button to Search Maven Central...
// MAGIC   1. Search for "spark-cassandra-connector"
// MAGIC   1. Choose your version and hit "select" on the right side
// MAGIC   1. Click the "Create Library" button
// MAGIC 1. On the following screen, choose to attach to all clusters or only specific clusters
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ##Create the init script
// MAGIC
// MAGIC Do once or when there is a change to the Cassandra cluster IP(s).

// COMMAND ----------

val cassandraHostIP = "172.16.1.4"

// Create script folder
dbutils.fs.mkdirs("/databricks/scripts")
// Add init script that adds the Cassandra hostname to all worker nodes
dbutils.fs.put(s"/databricks/scripts/cassandra.sh",
  s"""
     #!/usr/bin/bash
     echo '[driver]."spark.cassandra.connection.host" = "$cassandraHostIP"' >> /home/ubuntu/databricks/common/conf/cassandra.conf
   """.trim, true)

// COMMAND ----------

// Check to verify the init script file was created
dbutils.fs.head("/databricks/scripts/cassandra.sh")


// COMMAND ----------

// MAGIC %md ## Run the init script
// MAGIC
// MAGIC 1. Configure the script for a cluster by referencing in the cluster **Init Scripts** tab.
// MAGIC 1. Restart the cluster to run the cluster init script.

// COMMAND ----------

// Redefining since this would run after a reset of cluster
val cassandraHostIP = "172.16.1.4"

// COMMAND ----------

//verify sparkconf is set properly--This will be used by the connector
spark.conf.get("spark.cassandra.connection.host")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Use Cassandra
// MAGIC ### For the next bits, create some tables for reading from and writing into
// MAGIC   * Create tables in Cassandra in test_keyspace by running cqlsh command
// MAGIC   
// MAGIC 1. **For the read example**
// MAGIC
// MAGIC   <code>CREATE table words_new (user  TEXT, word  TEXT, count INT, PRIMARY KEY (user, word));</code>
// MAGIC
// MAGIC   <code>INSERT INTO words_new (user, word, count ) VALUES ( 'Russ', 'dino', 10 );</code>
// MAGIC
// MAGIC   <code>INSERT INTO words_new (user, word, count ) VALUES ( 'Russ', 'fad', 5 );</code>
// MAGIC
// MAGIC   <code>INSERT INTO words_new (user, word, count ) VALUES ( 'Sam', 'alpha', 3 );</code>
// MAGIC
// MAGIC   <code>INSERT INTO words_new (user, word, count ) VALUES ( 'Zebra', 'zed', 100 );</code>
// MAGIC
// MAGIC 1. **For the write example**
// MAGIC
// MAGIC   <code>CREATE table employee_new (id  TEXT, dep_id  TEXT, age INT, PRIMARY KEY (id));</code>

// COMMAND ----------

// MAGIC %md
// MAGIC ##Read Cassandra

// COMMAND ----------

// MAGIC %md
// MAGIC ## Dataframe Read the Cassandra table roles

// COMMAND ----------

// Start with a system table 
val df = spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "roles", "keyspace" -> "system_auth"))
  .load
df.explain

// COMMAND ----------

display(df)

// COMMAND ----------

// Using the table you created above
val df = spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "words_new", "keyspace" -> "test_keyspace"))
  .load
df.explain

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Write Cassandra

// COMMAND ----------

// Create some sample data
import org.apache.spark.sql.functions._
val employee1 = spark.range(0, 3).select($"id".as("id"), (rand() * 3).cast("int").as("dep_id"), (rand() * 40 + 20).cast("int").as("age"))

// COMMAND ----------

// Insert into your col-store/table
import org.apache.spark.sql.cassandra._

employee1.write
  .format("org.apache.spark.sql.cassandra")
  .mode("overwrite")
  .options(Map( "table" -> "employee_new", "keyspace" -> "test_keyspace"))
  .save()
