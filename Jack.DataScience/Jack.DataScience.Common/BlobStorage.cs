using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace Jack.DataScience.Common
{
    public class BlobStorage
    {
        private readonly AzureStorageOptions options;
        public BlobStorage(AzureStorageOptions options)
        {
            this.options = options;
        }
        public async Task WriteFileTo(
            string path,
            object value
            )
        {
            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            if (CloudStorageAccount.TryParse(options.ConnectionString, out storageAccount))
            {
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                cloudBlobContainer = cloudBlobClient.GetContainerReference(options.Container);

                await cloudBlobContainer.CreateIfNotExistsAsync();

                BlobContainerPermissions permissions = new BlobContainerPermissions
                {
                    PublicAccess = BlobContainerPublicAccessType.Blob
                };

                await cloudBlobContainer.SetPermissionsAsync(permissions);
                var blob = cloudBlobContainer.GetBlockBlobReference(path);

                // write to the azure blob
                await blob.UploadTextAsync(JsonConvert.SerializeObject(value));
            }
        }

        public async Task<CloudBlobContainer> GetContainer()
        {
            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            if (CloudStorageAccount.TryParse(options.ConnectionString, out storageAccount))
            {
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                cloudBlobContainer = cloudBlobClient.GetContainerReference(options.Container);

                await cloudBlobContainer.CreateIfNotExistsAsync();

                return cloudBlobContainer;
            }

            return null;
        }

        public async Task<bool> Exists(string path)
        {
            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            if (CloudStorageAccount.TryParse(options.ConnectionString, out storageAccount))
            {
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                cloudBlobContainer = cloudBlobClient.GetContainerReference(options.Container);

                await cloudBlobContainer.CreateIfNotExistsAsync();

                BlobContainerPermissions permissions = new BlobContainerPermissions
                {
                    PublicAccess = BlobContainerPublicAccessType.Blob
                };

                await cloudBlobContainer.SetPermissionsAsync(permissions);
                var blob = cloudBlobContainer.GetBlockBlobReference(path);
                // check if path exits
                return await blob.ExistsAsync();
            }
            throw new Exception($@"Error in connecting ");
        }

        public async Task<IEnumerable<IListBlobItem>> List()
        {
            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            if (CloudStorageAccount.TryParse(options.ConnectionString, out storageAccount))
            {
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                cloudBlobContainer = cloudBlobClient.GetContainerReference(options.Container);

                await cloudBlobContainer.CreateIfNotExistsAsync();

                BlobContainerPermissions permissions = new BlobContainerPermissions
                {
                    PublicAccess = BlobContainerPublicAccessType.Blob
                };

                await cloudBlobContainer.SetPermissionsAsync(permissions);
                return cloudBlobContainer.AllListBlobItems();
            }
            throw new Exception($@"Error in connecting ");
        }

        public async Task<string> DownloadAsString(string path)
        {
            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            if (CloudStorageAccount.TryParse(options.ConnectionString, out storageAccount))
            {
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                cloudBlobContainer = cloudBlobClient.GetContainerReference(options.Container);

                await cloudBlobContainer.CreateIfNotExistsAsync();

                BlobContainerPermissions permissions = new BlobContainerPermissions
                {
                    PublicAccess = BlobContainerPublicAccessType.Blob
                };

                await cloudBlobContainer.SetPermissionsAsync(permissions);
                var blob = cloudBlobContainer.GetBlockBlobReference(path);
                // check if path exits
                if(!await blob.ExistsAsync())
                {
                    throw new Exception($@"Blob does not exists.");
                }
                return await blob.DownloadTextAsync();
            }
            throw new Exception($@"Error in connecting ");
        }

        public async Task<bool> Delete(string path)
        {
            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            if (CloudStorageAccount.TryParse(options.ConnectionString, out storageAccount))
            {
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                cloudBlobContainer = cloudBlobClient.GetContainerReference(options.Container);

                await cloudBlobContainer.CreateIfNotExistsAsync();

                BlobContainerPermissions permissions = new BlobContainerPermissions
                {
                    PublicAccess = BlobContainerPublicAccessType.Blob
                };

                await cloudBlobContainer.SetPermissionsAsync(permissions);
                var blob = cloudBlobContainer.GetBlockBlobReference(path);
                // check if path exits
                await blob.DeleteIfExistsAsync();
                return true;
            }
            throw new Exception($@"Error in connecting ");
        }

        public async Task<bool> Rename(string oldPath, string newPath)
        {
            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            if (CloudStorageAccount.TryParse(options.ConnectionString, out storageAccount))
            {
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                cloudBlobContainer = cloudBlobClient.GetContainerReference(options.Container);

                await cloudBlobContainer.CreateIfNotExistsAsync();

                BlobContainerPermissions permissions = new BlobContainerPermissions
                {
                    PublicAccess = BlobContainerPublicAccessType.Blob
                };

                await cloudBlobContainer.SetPermissionsAsync(permissions);
                var oldBlob = cloudBlobContainer.GetBlockBlobReference(oldPath);
                var newBlob = cloudBlobContainer.GetBlockBlobReference(newPath);

                // check if path exits
                if(await oldBlob.ExistsAsync())
                {
                    await newBlob.StartCopyAsync(oldBlob);
                    await oldBlob.DeleteIfExistsAsync();
                }
                return true;
            }
            throw new Exception($@"Error in connecting ");
        }
    }
}
