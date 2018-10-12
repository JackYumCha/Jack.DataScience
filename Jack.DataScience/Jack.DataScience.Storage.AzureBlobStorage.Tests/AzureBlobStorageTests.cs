using System;
using Xunit;
using Jack.DataScience.Common;
using Jack.DataScience.Storage.AzureBlobStorage;
using Autofac;

namespace Jack.DataScience.Storage.AzureBlobStorage.Tests
{
    public class AzureBlobStorageTests
    {
        [Theory(DisplayName = "Create Azure Blob Storage File")]
        [InlineData("myFirstFile.txt", "testcontainer")]
        public async void CreateAzureBlobStorageFile(string filename, string blobContainer)
        {
            AutoFacContainer container = new AutoFacContainer();
            container.RegisterOptions<AzureBlobStorageOptions>();
            container.ContainerBuilder.RegisterModule<AzureBlobStorageModule>();
            var servicesContainer = container.ContainerBuilder.Build();
            var api = servicesContainer.Resolve<AzureBlobStorageAPI>();
            await api.UploadAsJson(filename, new
            {
                Name = "Jack",
                Value = "Test"
            }, blobContainer);
        }

        [Theory(DisplayName = "Delete Azure Blob Storage File")]
        [InlineData("myFirstFile.txt", "testcontainer")]
        public async void DeleteAzureBlobStorageFile(string filename, string blobContainer)
        {
            AutoFacContainer container = new AutoFacContainer();
            container.RegisterOptions<AzureBlobStorageOptions>();
            container.ContainerBuilder.RegisterModule<AzureBlobStorageModule>();
            var servicesContainer = container.ContainerBuilder.Build();
            var api = servicesContainer.Resolve<AzureBlobStorageAPI>();
            await api.Delete(filename, blobContainer);
        }
    }
}
