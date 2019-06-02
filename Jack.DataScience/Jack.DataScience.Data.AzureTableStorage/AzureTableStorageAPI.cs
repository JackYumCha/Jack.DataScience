using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;


namespace Jack.DataScience.Data.AzureTableStorage
{
    public class AzureTableStorageAPI
    {
        private readonly AzureTableStorageOptions azureTableStorageOptions;
        private readonly CloudStorageAccount cloudStorageAccount;
        private readonly CloudTableClient cloudTableClient;

        public AzureTableStorageAPI(AzureTableStorageOptions azureTableStorageOptions)
        {
            this.azureTableStorageOptions = azureTableStorageOptions;

            CloudStorageAccount storageAccount = null;
            if (CloudStorageAccount.TryParse(azureTableStorageOptions.ConnectionString, out storageAccount))
            {
                cloudStorageAccount = storageAccount;
            }

            cloudTableClient = storageAccount.CreateCloudTableClient();
        }

        private async Task<CloudTable> Table(string tableName)
        {
            var tableReference = cloudTableClient.GetTableReference(tableName);
            await tableReference.CreateIfNotExistsAsync();
            return tableReference;
        }

        public async Task<TTableEntity> Put<TTableEntity>(TTableEntity tableEntity, string tableName = null)
            where TTableEntity: TableEntity
        {
            if (string.IsNullOrWhiteSpace(tableName)) tableName = azureTableStorageOptions.Table;
            var table = await Table(tableName);
            var result = await table.ExecuteAsync(TableOperation.Insert(tableEntity));
            return result.Result as TTableEntity;
        }

        public async Task<TTableEntity> Replace<TTableEntity>(TTableEntity tableEntity, string tableName = null)
    where TTableEntity : TableEntity
        {
            if (string.IsNullOrWhiteSpace(tableName)) tableName = azureTableStorageOptions.Table;
            var table = await Table(tableName);
            var result = await table.ExecuteAsync(TableOperation.Replace(tableEntity));
            return result.Result as TTableEntity;
        }

        public async Task<TTableEntity> Delete<TTableEntity>(TTableEntity tableEntity, string tableName = null)
    where TTableEntity : TableEntity
        {
            if (string.IsNullOrWhiteSpace(tableName)) tableName = azureTableStorageOptions.Table;
            var table = await Table(tableName);
            var result = await table.ExecuteAsync(TableOperation.Delete(tableEntity));
            return result.Result as TTableEntity;
        }

        public async Task<TTableEntity> Upsert<TTableEntity>(TTableEntity tableEntity, string tableName = null)
    where TTableEntity : TableEntity
        {
            if (string.IsNullOrWhiteSpace(tableName)) tableName = azureTableStorageOptions.Table;
            var table = await Table(tableName);
            var result = await table.ExecuteAsync(TableOperation.InsertOrReplace(tableEntity));
            return result.Result as TTableEntity;
        }


        public async Task<TTableEntity> Get<TTableEntity>(string partitionKey, string rowKey, string tableName = null)
            where TTableEntity : TableEntity
        {
            if (string.IsNullOrWhiteSpace(tableName)) tableName = azureTableStorageOptions.Table;
            var table = await Table(tableName);
            var result = await table.ExecuteAsync(TableOperation.Retrieve<TTableEntity>(partitionKey, rowKey));
            return result.Result as TTableEntity;
        }

    }
}
