using System;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using Jack.DataScience.Data.CSV;
using Jack.DataScience.Data.Parquet;
using CsvHelper.Configuration;

namespace Jack.DataScience.Storage.AWSS3.Extensions
{
    public static class S3Extensions
    {
        public static async Task WriteCsv<T>(this AWSS3API awsS3, IEnumerable<T> items, string key, string bucket = null) where T : class
        {
            using(MemoryStream csvStream = new MemoryStream())
            {
                csvStream.WriteCsv(items);
                using(MemoryStream writeStream = new MemoryStream(csvStream.ToArray()))
                {
                    await awsS3.Upload(key, writeStream, bucket);
                }
            }
        }

        public static async Task WriteParquet<T>(this AWSS3API awsS3, IEnumerable<T> items, string key, string bucket = null) where T : class
        {
            using (MemoryStream csvStream = new MemoryStream())
            {
                csvStream.WriteParquet(items);
                using (MemoryStream writeStream = new MemoryStream(csvStream.ToArray()))
                {
                    await awsS3.Upload(key, writeStream, bucket);
                }
            }
        }

        public static async Task<List<T>> ReadCsv<T>(this AWSS3API awsS3, string key, string bucket = null, Configuration configuration = null) where T:class
        {
            using (Stream readStream = await awsS3.OpenReadAsync(key, bucket))
            {
                return readStream.ReadCsv<T>(configuration);
            }
        }

        private static Regex parquetSuffix = new Regex(@"\.parquet", RegexOptions.IgnoreCase);

        /// <summary>
        /// write parquet files in parallel
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="awsS3"></param>
        /// <param name="items"></param>
        /// <param name="partitionSize"></param>
        /// <param name="digitCount"></param>
        /// <param name="key"></param>
        /// <param name="bucket"></param>
        /// <returns></returns>
        public static async Task<List<string>> WriteParquetPerPartition<T>(this AWSS3API awsS3, IEnumerable<T> items, int partitionSize, int digitCount,
            string key, string bucket = null) where T : class
        {
            key = parquetSuffix.Replace(key, "");
            List<string> files = new List<string>();
            var writeTasks = items.SplitIntoPartitions(partitionSize).Select(async (partition, index) =>
            {
                if(partition.Count > 0)
                {
                    var fileKey = $"{key}-{index.ToString().PadLeft(digitCount, '0')}.parquet";
                    await awsS3.WriteParquet(partition, fileKey, bucket);
                    files.Add(fileKey);
                }
            }).ToArray();
            await Task.WhenAll(writeTasks);
            return files;
        }

        public static async Task<IEnumerable<T>> ReadParquet<T>(this AWSS3API awsS3, string key, string bucket = null) where T: class, new()
        {
            using(Stream readStream = await awsS3.OpenReadAsync(key, bucket))
            {
                return readStream.ReadParquet<T>();
            }
        }

        public static async Task<IEnumerable<T>> ReadParquetPartitions<T>(this AWSS3API awsS3, string prefix, string bucket = null) where T: class, new()
        {
            var objects = await awsS3.ListAllObjectsInBucket(bucket: bucket, prefix: prefix);

            var readTasks = objects.Select(async obj =>
            {
                return await awsS3.ReadParquet<T>(obj.Key, obj.BucketName);
            }).ToArray();

            var results = await Task.WhenAll(readTasks);
            return results.Aggregate(new List<T>(), (seed, items) =>
            {
                seed.AddRange(items);
                return seed;
            });
        }
    }
}
