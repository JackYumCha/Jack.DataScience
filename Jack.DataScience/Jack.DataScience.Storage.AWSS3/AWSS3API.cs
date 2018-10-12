using System;
using System.Text;
using System.IO;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Amazon.S3.Transfer;
using Amazon;
using Newtonsoft.Json;

namespace Jack.DataScience.Storage.AWSS3
{
    public class AWSS3API
    {
        private readonly AWSS3Options awsS3Options;

        public AWSS3API(AWSS3Options awsS3Options)
        {
            this.awsS3Options = awsS3Options;
        }

        public AmazonS3Client CreateClient() => 
            new AmazonS3Client(new BasicAWSCredentials(awsS3Options.Key, awsS3Options.Secret), RegionEndpoint.GetBySystemName(awsS3Options.Region));


        public async Task<bool> BucketExists(string name = null)
        {
            string bucketName = null;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                var exists = await AmazonS3Util.DoesS3BucketExistAsync(client, bucketName);
                return exists;
            }
        }

        public async Task CreateBucketIfNotExists(string name = null)
        {
            string bucketName = null;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                var exists = await AmazonS3Util.DoesS3BucketExistAsync(client, bucketName);
                if (!exists)
                {
                    PutBucketRequest request = new PutBucketRequest()
                    {
                        BucketName = name,
                        BucketRegionName = awsS3Options.Region
                    };
                    var response = await client.PutBucketAsync(request);
                }
            }
        }

        public async Task DeleteBucket(string name = null)
        {
            string bucketName = null;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                var exists = await AmazonS3Util.DoesS3BucketExistAsync(client, bucketName);
                if (exists)
                {
                    // make all objects expire so as to empty the bucket
                    await client.PutLifecycleConfigurationAsync(new PutLifecycleConfigurationRequest()
                    {
                        BucketName = bucketName,
                        Configuration = new LifecycleConfiguration()
                        {
                            Rules = {
                                    new LifecycleRule()
                                    {
                                        Expiration = new LifecycleRuleExpiration()
                                        {
                                            Date = DateTime.Now.AddDays(-10)
                                        },
                                        NoncurrentVersionExpiration = new LifecycleRuleNoncurrentVersionExpiration()
                                        {
                                            NoncurrentDays = 0
                                        }
                                    }
                                }
                        }
                    });

                    // may need to iterate the objects for deleting
                    //string continuationToken = null;
                    //do
                    //{
                    //    var response = await client.ListObjectsV2Async(new ListObjectsV2Request()
                    //    {
                    //        BucketName = bucketName,
                    //        ContinuationToken = continuationToken,
                    //    });
                    //    continuationToken = response.ContinuationToken;
                    //    foreach(var s3Object in response.S3Objects)
                    //    {

                    //        var deleteResponse = await client.DeleteObjectAsync(bucketName, s3Object.Key);
                    //    }
                    //}
                    //while (continuationToken != null);

                    await client.DeleteBucketAsync(new DeleteBucketRequest()
                    {
                        BucketName = bucketName,
                        BucketRegion = awsS3Options.Region
                    });
                }
            }
        }

        public async Task<List<S3Object>> ListAllObjectsInBucket(string name = null)
        {
            List<S3Object> objects = new List<S3Object>();
            string bucketName = null;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                var exists = await AmazonS3Util.DoesS3BucketExistAsync(client, bucketName);
                if (exists)
                {
                    string continuationToken = null;
                    do
                    {
                        var response = await client.ListObjectsV2Async(new ListObjectsV2Request()
                        {
                            BucketName = bucketName,
                            ContinuationToken = continuationToken
                        });
                        continuationToken = response.ContinuationToken;
                        objects.AddRange(response.S3Objects);
                    }
                    while (continuationToken != null);
                }
            }
            return objects;
        }

        public async Task WriteAsJson(string path, object value)
        {
            using (AmazonS3Client client = CreateClient())
            {
             

            }
        }

        public async Task Upload(string key, Stream stream, string bucket = null)
        {
            string bucketName = null;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                TransferUtility transferUtility = new TransferUtility(client);
                await transferUtility.UploadAsync(stream, bucketName, key);
            }
        }

        public async Task UploadAsJson(string key, object value, string bucket = null)
        {
            string bucketName = null;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                TransferUtility transferUtility = new TransferUtility(client);
                using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(value))))
                {
                    await transferUtility.UploadAsync(stream, bucketName, key);
                }
            }
        }

    }
}
