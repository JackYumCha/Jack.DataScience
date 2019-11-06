using System;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;
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
using System.Diagnostics;

namespace Jack.DataScience.Storage.AWSS3
{
    public class AWSS3API
    {
        private readonly AWSS3Options awsS3Options;

        public AWSS3API(AWSS3Options awsS3Options)
        {
            this.awsS3Options = awsS3Options;
        }

        public AWSS3Options Options
        {
            get => awsS3Options;
        }

        public AmazonS3Client CreateClient() =>
            new AmazonS3Client(new BasicAWSCredentials(awsS3Options.Key, awsS3Options.Secret), RegionEndpoint.GetBySystemName(awsS3Options.Region));


        public async Task<bool> BucketExists(string name = null)
        {
            string bucketName = name;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                var exists = await AmazonS3Util.DoesS3BucketExistAsync(client, bucketName);
                return exists;
            }
        }

        public async Task<bool> FileExists(string key, string bucket = null)
        {
            if (bucket == null) bucket = awsS3Options.Bucket;
            try
            {
                using (AmazonS3Client client = CreateClient())
                {
                    await client.GetObjectMetadataAsync(new GetObjectMetadataRequest()
                    {
                        BucketName = bucket,
                        Key = key
                    });
                }
                return true;
            }
            catch (AmazonS3Exception ex)
            {
                if (ex.StatusCode == HttpStatusCode.NotFound)
                    return false;
                throw;
            }
        }

        public async Task CreateBucket(string name = null)
        {
            string bucketName = name;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                PutBucketRequest request = new PutBucketRequest()
                {
                    BucketName = name,
                    BucketRegionName = awsS3Options.Region
                };
                var response = await client.PutBucketAsync(request);
            }
        }

        public async Task CreateBucketIfNotExists(string name = null)
        {
            string bucketName = name;
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
            string bucketName = name;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                var exists = await AmazonS3Util.DoesS3BucketExistAsync(client, bucketName);
                if (exists)
                {
                    // make all objects expire so as to empty the bucket
                    //await client.PutLifecycleConfigurationAsync(new PutLifecycleConfigurationRequest()
                    //{
                    //    BucketName = bucketName,
                    //    Configuration = new LifecycleConfiguration()
                    //    {
                    //        Rules = {
                    //                new LifecycleRule()
                    //                {
                    //                    Expiration = new LifecycleRuleExpiration()
                    //                    {
                    //                        Date = DateTime.Now.AddDays(-10)
                    //                    },
                    //                    NoncurrentVersionExpiration = new LifecycleRuleNoncurrentVersionExpiration()
                    //                    {
                    //                        NoncurrentDays = 0
                    //                    }
                    //                }
                    //            }
                    //    }
                    //});

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

        public async Task<List<string>> ListPaths(string prefix, string delimiter = "/", string bucket = null)
        {
            List<string> objects = new List<string>();
            string bucketName = bucket;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                var exists = await AmazonS3Util.DoesS3BucketExistAsync(client, bucketName);
                if (exists)
                {
                    string continuationToken = null;
                    int prefixLength = prefix.Length;
                    do
                    {
                        var response = await client.ListObjectsV2Async(new ListObjectsV2Request()
                        {
                            BucketName = bucketName,
                            ContinuationToken = continuationToken,
                            Delimiter = delimiter,
                            Prefix = prefix
                        });
                        continuationToken = response.NextContinuationToken;
                        objects.AddRange(response.CommonPrefixes.Select(p => p.Substring(prefixLength)));
                    }
                    while (continuationToken != null);
                }
            }
            return objects;
        }

        public async Task<List<string>> ListFiles(string prefix, string delimiter = "/", string bucket = null)
        {
            List<string> objects = new List<string>();
            string bucketName = bucket;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                var exists = await AmazonS3Util.DoesS3BucketExistAsync(client, bucketName);
                if (exists)
                {
                    string continuationToken = null;
                    int prefixLength = prefix.Length;
                    do
                    {
                        var response = await client.ListObjectsV2Async(new ListObjectsV2Request()
                        {
                            BucketName = bucketName,
                            ContinuationToken = continuationToken,
                            Delimiter = delimiter,
                            Prefix = prefix
                        });
                        continuationToken = response.NextContinuationToken;
                        objects.AddRange(response.S3Objects.Select(o => o.Key.Substring(prefixLength)));
                    }
                    while (continuationToken != null);
                }
            }
            return objects;
        }

        public async Task<List<S3Object>> ListAllObjectsInBucket(string bucket = null, string prefix = null)
        {
            List<S3Object> objects = new List<S3Object>();
            string bucketName = bucket;
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
                            ContinuationToken = continuationToken,
                            Prefix = prefix
                        });
                        continuationToken = response.NextContinuationToken;
                        objects.AddRange(response.S3Objects);
                    }
                    while (continuationToken != null);
                }
            }
            return objects;
        }

        public async Task Delete(string key, string bucket = null, string versionId = null)
        {
            string bucketName = bucket;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                await client.DeleteObjectAsync(new DeleteObjectRequest()
                {
                    BucketName = bucketName,
                    Key = key,
                    VersionId = versionId
                });
            }
        }

        public async Task Delete(IEnumerable<string> keys, string bucket = null, string versionId = null)
        {
            string bucketName = bucket;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            if (keys.Any())
            {
                using (AmazonS3Client client = CreateClient())
                {
                    var response = await client.DeleteObjectsAsync(new DeleteObjectsRequest()
                    {
                        BucketName = bucketName,
                        Objects = keys.Select(key => new KeyVersion() { Key = key, VersionId = versionId }).ToList(),
                        Quiet = true,
                    });
                }
            }
        }

        public async Task Copy(string sourceKey, string desitnationKey, string sourceBucket = null, string desitnationBucket = null, string sourceVersionId = null)
        {
            string sourceBucketName = sourceBucket;
            if (sourceBucketName == null) sourceBucketName = awsS3Options.Bucket;
            string destinationBucketName = desitnationBucket;
            if (destinationBucketName == null) destinationBucketName = sourceBucketName;
            using (AmazonS3Client client = CreateClient())
            {
                TransferUtility transferUtility = new TransferUtility(client);
                await client.CopyObjectAsync(new CopyObjectRequest()
                {
                    SourceBucket = sourceBucketName,
                    DestinationBucket = destinationBucketName,
                    SourceKey = sourceKey,
                    DestinationKey = desitnationKey,
                    SourceVersionId = sourceVersionId
                });
            }
        }

        public async Task Upload(string key, Stream stream, string bucket = null)
        {
            string bucketName = bucket;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                TransferUtility transferUtility = new TransferUtility(client);
                await transferUtility.UploadAsync(stream, bucketName, key);
            }
        }

        /// <summary>
        /// upload the file to S3 bucket with public access
        /// </summary>
        /// <param name="key"></param>
        /// <param name="stream"></param>
        /// <param name="bucket"></param>
        /// <returns></returns>
        public async Task UploadAsPublic(string key, Stream stream, string bucket = null)
        {
            string bucketName = bucket;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                TransferUtility transferUtility = new TransferUtility(client);
                var request = new TransferUtilityUploadRequest()
                {
                    InputStream = stream,
                    Key = key,
                    BucketName = bucketName,
                    CannedACL = S3CannedACL.PublicRead
                };
                await transferUtility.UploadAsync(request);
            }
        }

        public async Task UploadAsJson(string key, object value, string bucket = null, JsonSerializerSettings jsonSerializerSettings = null)
        {
            string bucketName = bucket;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                TransferUtility transferUtility = new TransferUtility(client);
                byte[] data = null;
                if(jsonSerializerSettings == null)
                {
                    data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(value));
                }
                else
                {
                    data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(value, jsonSerializerSettings));
                }
                using (MemoryStream stream = new MemoryStream(data))
                {
                    await transferUtility.UploadAsync(stream, bucketName, key);
                }
            }
        }

        public async Task UploadAsGZipJson(string key, object value, string bucket = null, JsonSerializerSettings jsonSerializerSettings = null)
        {
            string bucketName = bucket;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                TransferUtility transferUtility = new TransferUtility(client);
                byte[] data = null;
                if (jsonSerializerSettings == null)
                {
                    data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(value));
                }
                else
                {
                    data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(value, jsonSerializerSettings));
                }
                using (MemoryStream stream = new MemoryStream())
                {
                    using (GZipStream gZipStream = new GZipStream(stream, CompressionLevel.Optimal))
                    {
                        gZipStream.Write(data, 0, data.Length);
                    }
                    using(MemoryStream streamToUpload = new MemoryStream(stream.ToArray()))
                    {
                        await transferUtility.UploadAsync(streamToUpload, bucketName, key);
                    }
                }
            }
        }

        public  async Task UploadGZip(string key, byte[] data, string bucket = null)
        {
            string bucketName = bucket;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                TransferUtility transferUtility = new TransferUtility(client);
                using (MemoryStream stream = new MemoryStream())
                {
                    using (GZipStream gZipStream = new GZipStream(stream, CompressionLevel.Optimal))
                    {
                        gZipStream.Write(data, 0, data.Length);
                    }
                    using (MemoryStream streamToUpload = new MemoryStream(stream.ToArray()))
                    {
                        await transferUtility.UploadAsync(streamToUpload, bucketName, key);
                    }
                }
            }
        }

        public async Task<T> ReadFromJson<T>(string key, string bucket = null, JsonSerializerSettings jsonSerializerSettings = null) where T: class, new()
        {
            var bytes = await ReadAsBytes(key, bucket);
            if (bytes == null) return null;
            var value = Encoding.UTF8.GetString(bytes);
            return jsonSerializerSettings == null ? JsonConvert.DeserializeObject<T>(value) : JsonConvert.DeserializeObject<T>(value, jsonSerializerSettings);
        }

        public async Task<Stream> OpenReadAsync(string key, string bucket = null)
        {
            string bucketName = bucket;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                TransferUtility transferUtility = new TransferUtility(client);
                return await transferUtility.OpenStreamAsync(bucketName, key);
            }
        }

        /// <summary>
        /// Read file as bytes
        /// </summary>
        /// <param name="key"></param>
        /// <param name="bucket"></param>
        /// <returns></returns>
        public async Task<byte[]> ReadAsBytes(string key, string bucket = null)
        {
            string bucketName = bucket;
            if (bucketName == null) bucketName = awsS3Options.Bucket;
            using (AmazonS3Client client = CreateClient())
            {
                TransferUtility transferUtility = new TransferUtility(client);
                using (var stream = await transferUtility.OpenStreamAsync(bucketName, key))
                {
                    using(MemoryStream memoryStream = new MemoryStream())
                    {
                        await stream.CopyToAsync(memoryStream);
                        return memoryStream.ToArray();
                    }
                }
            }
        }

        /// <summary>
        /// Read file as string
        /// </summary>
        /// <param name="key"></param>
        /// <param name="bucket"></param>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public async Task<string> ReadAsString(string key, string bucket = null, Encoding encoding = null)
        {
            var bytes = await ReadAsBytes(key, bucket);
            if (bytes == null) return null;
            if (encoding == null) encoding = Encoding.UTF8;
            var value = encoding.GetString(bytes);
            return value;
        }
    }
}
