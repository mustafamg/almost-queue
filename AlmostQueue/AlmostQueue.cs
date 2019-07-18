using Microsoft.Azure.Cosmos.Table;
using System;
using System.Threading.Tasks;

namespace FreedomTools
{
    public class AlmostQueue<T> where T : AlmostTable
    {
        const string ProfilePicPath = "system/userprofilepictures/profile-radar-color{0}.png";

        CloudStorageAccount storageAccount;
        private CloudStorageAccount CreateStorageClient()
        {
            if (storageAccount != null)
                return storageAccount;
            // Retrieve the connection string for use with the application. The storage connection string is stored
            // in an environment variable on the machine running the application called storageconnectionstring.
            // If the environment variable is created after the application is launched in a console or with Visual
            // Studio, the shell or application needs to be closed and reloaded to take the environment variable into account.
            string storageConnectionString = Environment.GetEnvironmentVariable("storageconnectionstring");
            // Check whether the connection string can be parsed.

            if (CloudStorageAccount.TryParse(storageConnectionString, out storageAccount))
            {
                return storageAccount;
            }
            else
            {
                // Otherwise, let the user know that they need to define the environment variable.
                throw new ApplicationException(
                     "A connection string has not been defined in the system environment variables. ");
            }
        }


        private async Task<CloudTable> GetCloudTable()
        {
            var storageAccount = CreateStorageClient();
            var tableClient = storageAccount.CreateCloudTableClient();
            var table = tableClient.GetTableReference("AlmostQueue");
            await table.CreateIfNotExistsAsync();

            return table;
        }

        public async Task<T> Push(TableEntity message)
        {
            var table = await GetCloudTable();
            if (message == null)
            {
                throw new ArgumentNullException("You must provide a message, null is not accepted as a valid value.");
            }
            try
            {
                // Create the InsertOrReplace table operation
                TableOperation insertOrMergeOperation = TableOperation.Insert(message);

                // Execute the operation.
                TableResult result = await table.ExecuteAsync(insertOrMergeOperation);
                T insertedMessage = result.Result as T;

                // Get the request units consumed by the current operation. RequestCharge of a TableResult is only applied to Azure CosmoS DB 
                if (result.RequestCharge.HasValue)
                {
                    Console.WriteLine("Request Charge of InsertOrMerge Operation: " + result.RequestCharge);
                }

                return insertedMessage;
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }


        public async Task<T> Pop(string messageId)
        {
            if (string.IsNullOrWhiteSpace(messageId))
            {
                throw new ArgumentNullException("You must provide a message, null is not accepted as a valid value.");
            }

            string partitionKeyCurrent = AlmostTable.GetTimelyKey();
            string partitionKeyPrevious = AlmostTable.GetTimelyKey(offset: -1);

            var table = await GetCloudTable();

            try
            {
                TableOperation retrieveOperation = TableOperation.Retrieve<T>(partitionKeyCurrent, messageId);
                TableResult result = await table.ExecuteAsync(retrieveOperation);
                if (result.Result == null)
                {
                    retrieveOperation = TableOperation.Retrieve<T>(partitionKeyPrevious, messageId);
                    result = await table.ExecuteAsync(retrieveOperation);
                }

                T entity = result.Result as T;
                if (result.Result != null)
                {
                    TableOperation deleteOperation = TableOperation.Delete(entity);
                    result = await table.ExecuteAsync(deleteOperation);
                }

                return entity;
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }
    }
}
