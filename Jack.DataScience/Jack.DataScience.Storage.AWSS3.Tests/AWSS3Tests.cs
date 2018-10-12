using System;
using Xunit;
using Jack.DataScience.Common;
using Autofac;

namespace Jack.DataScience.Storage.AWSS3.Tests
{
    public class AWSS3Tests
    {
        [Theory(DisplayName = "Create S3 Bucket")]
        [InlineData("jack-datascience-testbucket")]
        public async void CreateS3Bucket(string bucket)
        {
            AutoFacContainer container = new AutoFacContainer();
            container.RegisterOptions<AWSS3Options>();
            container.ContainerBuilder.RegisterModule<AWSS3Module>();
            var servicesContainer = container.ContainerBuilder.Build();
            var api = servicesContainer.Resolve<AWSS3API>();
            await api.CreateBucket(bucket);
        }

        [Theory(DisplayName = "Write S3 Object")]
        [InlineData("myfirstobj.txt", "jack-datascience-testbucket")]
        public async void WriteStringToS3Bucket(string key, string bucket)
        {
            AutoFacContainer container = new AutoFacContainer();
            container.RegisterOptions<AWSS3Options>();
            container.ContainerBuilder.RegisterModule<AWSS3Module>();
            var servicesContainer = container.ContainerBuilder.Build();
            var api = servicesContainer.Resolve<AWSS3API>();
            await api.UploadAsJson(key, new
            {
                Name = "Jack",
                Value = "First"
            }, bucket);
        }

        [Theory(DisplayName = "Delete S3 Object")]
        [InlineData("myfirstobj.txt", "jack-datascience-testbucket")]
        public async void DeleteS3Object(string key, string bucket)
        {
            AutoFacContainer container = new AutoFacContainer();
            container.RegisterOptions<AWSS3Options>();
            container.ContainerBuilder.RegisterModule<AWSS3Module>();
            var servicesContainer = container.ContainerBuilder.Build();
            var api = servicesContainer.Resolve<AWSS3API>();
            await api.Delete(key, bucket);
        }

        [Theory(DisplayName = "Delete S3 Bucket")]
        [InlineData("jack-datascience-testbucket")]
        public async void DeleteS3Bucket(string bucket)
        {
            AutoFacContainer container = new AutoFacContainer();
            container.RegisterOptions<AWSS3Options>();
            container.ContainerBuilder.RegisterModule<AWSS3Module>();
            var servicesContainer = container.ContainerBuilder.Build();
            var api = servicesContainer.Resolve<AWSS3API>();
            await api.DeleteBucket(bucket);
        }
    }
}
