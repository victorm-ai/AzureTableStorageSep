using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Azure;
using Microsoft.WindowsAzure.ServiceRuntime;
using System.Windows.Forms;

namespace AzureTableStorage.Models
{
    public class MyTableStorage
    {
        private string Account { get; set; }
        private string Key { get; set; }

        public MyTableStorage()
        {
            this.Account = "otheraccount";
            this.Key = "";
        }

        public void Create_a_table()
        {
            //Retrieve the storage account from the connection string.
            StorageCredentials Credentials = new StorageCredentials(this.Account, this.Key);            
            CloudStorageAccount storageAccount = new CloudStorageAccount(Credentials, false);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Retrieve a reference to the table.
            CloudTable table = tableClient.GetTableReference("Demo");

            // Create the table if it doesn't exist.
            table.CreateIfNotExists();
            MessageBox.Show("Table created", "Entity", MessageBoxButtons.OK);
        }

        public void Add_an_entity_to_a_table()
        {
            //Retrieve the storage account from the connection string.
            StorageCredentials Credentials = new StorageCredentials(this.Account, this.Key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(Credentials, false);


            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "Demo" table.
            CloudTable table = tableClient.GetTableReference("Demo");

            // Create a new customer entity.
            CustomerEntity customer1 = new CustomerEntity("Harp", "Walter");
            customer1.Email = "Walter@contoso.com";
            customer1.PhoneNumber = "425-555-0101";

            // Create the TableOperation object that inserts the customer entity.
            TableOperation insertOperation = TableOperation.Insert(customer1);

            // Execute the insert operation.
            table.Execute(insertOperation);
            MessageBox.Show("Entity added", "Entity", MessageBoxButtons.OK);
        }

        public void Add_a_batch_of_entities()
        {
            //Retrieve the storage account from the connection string.
            StorageCredentials Credentials = new StorageCredentials(this.Account, this.Key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(Credentials, false);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "Demo" table.
            CloudTable table = tableClient.GetTableReference("Demo");

            // Create the batch operation.
            TableBatchOperation batchOperation = new TableBatchOperation();

            // Create a customer entity and add it to the table.
            CustomerEntity customer1 = new CustomerEntity("Smith", "Jeff");
            customer1.Email = "Jeff@contoso.com";
            customer1.PhoneNumber = "425-555-0104";

            // Create another customer entity and add it to the table.
            CustomerEntity customer2 = new CustomerEntity("Smith", "Ben");
            customer2.Email = "Ben@contoso.com";
            customer2.PhoneNumber = "425-555-0102";

            // Add both customer entities to the batch insert operation.
            batchOperation.Insert(customer1);
            batchOperation.Insert(customer2);

            // Execute the batch operation.
            table.ExecuteBatch(batchOperation);
            MessageBox.Show("Several entities added", "Entity", MessageBoxButtons.OK);
        }

        public void Retrieve_all_entities_in_a_partition()
        {
            //Retrieve the storage account from the connection string.
            StorageCredentials Credentials = new StorageCredentials(this.Account, this.Key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(Credentials, false);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "Demo" table.
            CloudTable table = tableClient.GetTableReference("Demo");

            // Construct the query operation for all customer entities where PartitionKey="Smith".
            TableQuery<CustomerEntity> query = new TableQuery<CustomerEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Smith"));

            MessageBox.Show("Receiving entities by partition", "Entity", MessageBoxButtons.OK);

            // Print the fields for each customer.
            foreach (CustomerEntity entity in table.ExecuteQuery(query))
            {
                MessageBox.Show(entity.PartitionKey + " " +
                                entity.RowKey + " " +
                                entity.Email + " " +
                                entity.PhoneNumber + " ",
                                "Entity", MessageBoxButtons.OK
                                );
                Console.WriteLine("{0}, {1}\t{2}\t{3}", entity.PartitionKey, entity.RowKey,
                    entity.Email, entity.PhoneNumber);
            }
        }

        public void Retrieve_a_range_of_entities_in_a_partition()
        {
            //Retrieve the storage account from the connection string.
            StorageCredentials Credentials = new StorageCredentials(this.Account, this.Key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(Credentials, false);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "Demo" table.
            CloudTable table = tableClient.GetTableReference("Demo");

            // Create the table query.
            TableQuery<CustomerEntity> rangeQuery = new TableQuery<CustomerEntity>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Smith"),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, "E")));

            MessageBox.Show("Receiving entities by range", "Entity", MessageBoxButtons.OK);

            // Loop through the results, displaying information about the entity.
            foreach (CustomerEntity entity in table.ExecuteQuery(rangeQuery))
            {
                MessageBox.Show(entity.PartitionKey + " " +
                               entity.RowKey + " " +
                               entity.Email + " " +
                               entity.PhoneNumber + " ",
                               "Entity", MessageBoxButtons.OK
                               );

                Console.WriteLine("{0}, {1}\t{2}\t{3}", entity.PartitionKey, entity.RowKey,
                    entity.Email, entity.PhoneNumber);
            }
        }

        public void Retrieve_a_single_entity()
        {
            //Retrieve the storage account from the connection string.
            StorageCredentials Credentials = new StorageCredentials(this.Account, this.Key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(Credentials, false);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "Demo" table.
            CloudTable table = tableClient.GetTableReference("Demo");

            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<CustomerEntity>("Smith", "Ben");

            // Execute the retrieve operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            MessageBox.Show("Receiving by single entity", "Entity", MessageBoxButtons.OK);

            // Print the phone number of the result.
            if (retrievedResult.Result != null)
            {
                MessageBox.Show(((CustomerEntity)retrievedResult.Result).PhoneNumber, "Entity", MessageBoxButtons.OK);
                Console.WriteLine(((CustomerEntity)retrievedResult.Result).PhoneNumber);
            }
                
            else
                Console.WriteLine("The phone number could not be retrieved.");
        }
        public void Replace_an_entity()
        {
            //Retrieve the storage account from the connection string.
            StorageCredentials Credentials = new StorageCredentials(this.Account, this.Key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(Credentials, false);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "Demo" table.
            CloudTable table = tableClient.GetTableReference("Demo");

            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<CustomerEntity>("Smith", "Ben");

            // Execute the operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Assign the result to a CustomerEntity object.
            CustomerEntity updateEntity = (CustomerEntity)retrievedResult.Result;

            if (updateEntity != null)
            {
                // Change the phone number.
                updateEntity.PhoneNumber = "425-555-0105";

                // Create the Replace TableOperation.
                TableOperation updateOperation = TableOperation.Replace(updateEntity);

                // Execute the operation.
                table.Execute(updateOperation);

                Console.WriteLine("Entity updated.");
                MessageBox.Show("Entity updated", "Entity", MessageBoxButtons.OK);
            }

            else
                Console.WriteLine("Entity could not be retrieved.");
        }

        public void Insert_or_replace_an_entity()
        {
            //Retrieve the storage account from the connection string.
            StorageCredentials Credentials = new StorageCredentials(this.Account, this.Key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(Credentials, false);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable object that represents the "Demo" table.
            CloudTable table = tableClient.GetTableReference("Demo");

            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<CustomerEntity>("Smith", "Ben");

            // Execute the operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Assign the result to a CustomerEntity object.
            CustomerEntity updateEntity = (CustomerEntity)retrievedResult.Result;

            if (updateEntity != null)
            {
                // Change the phone number.
                updateEntity.PhoneNumber = "425-555-1234";

                // Create the InsertOrReplace TableOperation.
                TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace(updateEntity);

                // Execute the operation.
                table.Execute(insertOrReplaceOperation);

                MessageBox.Show(updateEntity.PhoneNumber, "Entity", MessageBoxButtons.OK);

                Console.WriteLine("Entity was updated.");
                MessageBox.Show("Entity updated", "Entity", MessageBoxButtons.OK);
            }

            else
                Console.WriteLine("Entity could not be retrieved.");
        }

        public void Query_a_subset_of_entity_properties()
        {
            //Retrieve the storage account from the connection string.
            StorageCredentials Credentials = new StorageCredentials(this.Account, this.Key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(Credentials, false);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable that represents the "Demo" table.
            CloudTable table = tableClient.GetTableReference("Demo");

            // Define the query, and select only the Email property.
            TableQuery<DynamicTableEntity> projectionQuery = new TableQuery<DynamicTableEntity>().Select(new string[] { "Email" });

            // Define an entity resolver to work with the entity after retrieval.
            EntityResolver<string> resolver = (pk, rk, ts, props, etag) => props.ContainsKey("Email") ? props["Email"].StringValue : null;

            MessageBox.Show("Receiving entitues by subset", "Entity", MessageBoxButtons.OK);

            foreach (string projectedEmail in table.ExecuteQuery(projectionQuery, resolver, null, null))
            {
                MessageBox.Show(projectedEmail, "Entity", MessageBoxButtons.OK);
                Console.WriteLine(projectedEmail);
            }
        }

        public void Delete_an_entity()
        {
            //Retrieve the storage account from the connection string.
            StorageCredentials Credentials = new StorageCredentials(this.Account, this.Key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(Credentials, false);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable that represents the "Demo" table.
            CloudTable table = tableClient.GetTableReference("Demo");

            // Create a retrieve operation that expects a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<CustomerEntity>("Smith", "Ben");

            // Execute the operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Assign the result to a CustomerEntity.
            CustomerEntity deleteEntity = (CustomerEntity)retrievedResult.Result;

            // Create the Delete TableOperation.
            if (deleteEntity != null)
            {
                TableOperation deleteOperation = TableOperation.Delete(deleteEntity);

                // Execute the operation.
                table.Execute(deleteOperation);

                Console.WriteLine("Entity deleted.");
                MessageBox.Show("Entity deleted", "Entity", MessageBoxButtons.OK);
            }

            else
                Console.WriteLine("Could not retrieve the entity.");
        }

        public void Delete_a_table()
        {
            //Retrieve the storage account from the connection string.
            StorageCredentials Credentials = new StorageCredentials(this.Account, this.Key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(Credentials, false);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create the CloudTable that represents the "Demo" table.
            CloudTable table = tableClient.GetTableReference("Demo");

            // Delete the table it if exists.
            table.DeleteIfExists();
            MessageBox.Show("Table deleted", "Entity", MessageBoxButtons.OK);
        }
    }
}