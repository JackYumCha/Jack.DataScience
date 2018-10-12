using System;
using System.IO;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace Jack.DataScience.Storage.AzureBlobStorage
{
    public class AzureBlobStorageAPI
    {
        private readonly AzureBlobStorageOptions options;
        public AzureBlobStorageAPI(AzureBlobStorageOptions options)
        {
            this.options = options;
        }

        public async Task Upload(string path, Stream stream, string container = null)
        {
            string containerName = container;
            if (containerName == null)
            {
                containerName = options.Container;
            }

            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            if (CloudStorageAccount.TryParse(options.ConnectionString, out storageAccount))
            {
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);

                await cloudBlobContainer.CreateIfNotExistsAsync();

                BlobContainerPermissions permissions = new BlobContainerPermissions
                {
                    PublicAccess = BlobContainerPublicAccessType.Blob
                };

                await cloudBlobContainer.SetPermissionsAsync(permissions);
                var blob = cloudBlobContainer.GetBlockBlobReference(path);

                // write to the azure blob
                await blob.UploadFromStreamAsync(stream);
            }
        }

        public async Task UploadAsJson(string path, object value, string container = null)
        {
            string containerName = container;
            if (containerName == null)
            {
                containerName = options.Container;
            }

            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            if (CloudStorageAccount.TryParse(options.ConnectionString, out storageAccount))
            {
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);

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

        public async Task<CloudBlobContainer> GetOrCreateContainer(string container = null)
        {
            string containerName = container;
            if(containerName == null)
            {
                containerName = options.Container;
            }

            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            if (CloudStorageAccount.TryParse(options.ConnectionString, out storageAccount))
            {
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);

                await cloudBlobContainer.CreateIfNotExistsAsync();

                return cloudBlobContainer;
            }

            return null;
        }

        public async Task<bool> Exists(string path, string container = null)
        {

            string containerName = container;
            if (containerName == null)
            {
                containerName = options.Container;
            }

            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            if (CloudStorageAccount.TryParse(options.ConnectionString, out storageAccount))
            {
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);

                await cloudBlobContainer.CreateIfNotExistsAsync();

                BlobContainerPermissions permissions = new BlobContainerPermissions
                {
                    PublicAccess = BlobContainerPublicAccessType.Blob
                };

                await cloudBlobContainer.SetPermissionsAsync(permissions);
                var blob = cloudBlobContainer.GetBlockBlobReference(path);

                return await blob.ExistsAsync();
            }
            throw new Exception($@"Error in connecting ");
        }

        public async Task<IEnumerable<IListBlobItem>> List(string container = null)
        {
            string containerName = container;
            if (containerName == null)
            {
                containerName = options.Container;
            }

            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            if (CloudStorageAccount.TryParse(options.ConnectionString, out storageAccount))
            {
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);

                await cloudBlobContainer.CreateIfNotExistsAsync();

                BlobContainerPermissions permissions = new BlobContainerPermissions
                {
                    PublicAccess = BlobContainerPublicAccessType.Blob
                };

                await cloudBlobContainer.SetPermissionsAsync(permissions);
                return cloudBlobContainer.ListAllBlobItems();
            }
            throw new Exception($@"Error in connecting ");
        }

        public async Task<string> DownloadAsString(string path, string container = null)
        {
            string containerName = container;
            if (containerName == null)
            {
                containerName = options.Container;
            }

            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            if (CloudStorageAccount.TryParse(options.ConnectionString, out storageAccount))
            {
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);

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

        public async Task<bool> Delete(string path, string container = null)
        {
            string containerName = container;
            if (containerName == null)
            {
                containerName = options.Container;
            }

            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            if (CloudStorageAccount.TryParse(options.ConnectionString, out storageAccount))
            {
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);

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

        public async Task<bool> Rename(string oldPath, string newPath, string container = null)
        {
            string containerName = container;
            if (containerName == null)
            {
                containerName = options.Container;
            }

            CloudStorageAccount storageAccount = null;
            CloudBlobContainer cloudBlobContainer = null;

            if (CloudStorageAccount.TryParse(options.ConnectionString, out storageAccount))
            {
                CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

                cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);

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
