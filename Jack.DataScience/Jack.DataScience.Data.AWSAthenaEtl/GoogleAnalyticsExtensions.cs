using System;
using System.Linq;
using System.Diagnostics;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Google.Cloud.BigQuery.V2;
using Amazon;
using Amazon.S3;
using Amazon.S3.Transfer;
using Amazon.S3.Util;
using Amazon.S3.Model;
using Amazon.Runtime;
using Newtonsoft.Json;
using Jack.DataScience.Storage.AWSS3;
using Jack.DataScience.Data.AWSAthena;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    public static class GoogleAnalyticsExtensions
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="etlSettings"></param>
        /// <returns></returns>
        public static async Task<List<string>> TransferBigQueryResultByDate(this EtlSettings etlSettings, AWSAthenaAPI awsAthenaAPI, DateTime? useDate = null)
        {
            var result = new List<string>();

            var awsS3Api = etlSettings.CreateTargetS3API();
            var ga = etlSettings.GoogleAnalyticsQuerySource;

            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", $"{AppContext.BaseDirectory}/{ga.GoogleAnalyticsSettingFile}");

            BigQueryClient client = BigQueryClient.Create(ga.GoogleAnalyticsProjectId);

            string sql = ga.BigQuerySQL;
            var today = DateTime.UtcNow;
            if (useDate.HasValue) today = useDate.Value;
            string dateQueryKey = today.AddDays(-ga.DaysAgo).ToString(ga.DateFormat);
            string dateKey = today.AddDays(-ga.DaysAgo).ToString("yyyyMMdd");

            sql = sql.Replace("{date}", dateKey);

            var job = await client.CreateQueryJobAsync(sql, new List<BigQueryParameter>());

            BigQueryResults results = null;

            results = await client.GetQueryResultsAsync(job.Reference, new GetQueryResultsOptions()
            {
                StartIndex = 0,
                PageSize = 20000,
            });

            var enumerator = results.GetEnumerator();

            List<BigQueryRow> rows = new List<BigQueryRow>();

            int parquetIndex = 0;

            var targetS3 = etlSettings.CreateTargetS3API();

            while (enumerator.MoveNext())
            {
                rows.Add(enumerator.Current);
                if (rows.Count >= etlSettings.NumberOfItemsPerParquet)
                {
                    var s3key = etlSettings.MakeTargetS3Key(dateKey, "", false, parquetIndex);
                    await targetS3.WriteResultRowsToS3Bucket(rows, results, etlSettings, s3key);
                    result.Add($"s3://{etlSettings.TargetS3BucketName}/{s3key}");
                    parquetIndex += 1;
                }
            }

            // write what ever left less than 200000
            if(rows.Count > 0)
            {
                var s3key = etlSettings.MakeTargetS3Key(dateKey, "", false, parquetIndex);
                await targetS3.WriteResultRowsToS3Bucket(rows, results, etlSettings, s3key);
                result.Add($"s3://{etlSettings.TargetS3BucketName}/{s3key}");
                parquetIndex += 1;
            }

            {
                // load partition to athena table
                await awsAthenaAPI.LoadPartition(
                    $"`{etlSettings.AthenaDatabaseName}`.`{etlSettings.AthenaTableName}`",
                    $"`{etlSettings.DatePartitionKey}` = '{dateKey}'",
                    $"s3://{etlSettings.TargetS3BucketName}/{etlSettings.TargetS3Prefix}/{dateKey}/");
            }

            return result;
        }


        public static async Task GetBigQueryResultSampleByDate(this EtlSettings etlSettings, int lines)
        {
            var awsS3Api = etlSettings.CreateTargetS3API();
            var ga = etlSettings.GoogleAnalyticsQuerySource;

            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", $"{AppContext.BaseDirectory}/{ga.GoogleAnalyticsSettingFile}");

            BigQueryClient client = BigQueryClient.Create(ga.GoogleAnalyticsProjectId);

            string sql = ga.BigQuerySQL;

            string dateQueryKey = DateTime.Now.AddDays(-ga.DaysAgo).ToString(ga.DateFormat);

            // make sure the query is limited by 20
            sql = sql.Replace("{date}", dateQueryKey) + $"\nlimit {lines}";

            var job = await client.CreateQueryJobAsync(sql, new List<BigQueryParameter>());

            BigQueryResults results = null;

            results = await client.GetQueryResultsAsync(job.Reference, new GetQueryResultsOptions()
            {
                StartIndex = 0,
                PageSize = 20000,
            });

            var enumerator = results.GetEnumerator();

            List<BigQueryRow> rows = new List<BigQueryRow>();

            while (enumerator.MoveNext())
            {
                rows.Add(enumerator.Current);
            }

            // map schema to athena types

            etlSettings.Mappings = results.ToFieldMappings();
            var sample = new DataSample()
            {
                Rows = new List<DataRow>()
            };
            // convert big query data to sample data
            foreach (var row in rows)
            {
                sample.Rows.Add(new DataRow()
                {
                    Items = row.RawRow.F.Select(item =>
                    {
                        if(item.V == null)
                        {
                            return "";
                        }
                        else if(item.V.GetType() == typeof(DateTime))
                        {
                            return ((DateTime)item.V).ToString("o");
                        }
                        else if(item.V.GetType() == typeof(byte[]))
                        {
                            return Convert.ToBase64String((byte[])item.V);
                        }
                        else
                        {
                            return item.V.ToString();
                        }
                    }).ToList()
                });
            }
            etlSettings.Sample = sample;
        }

        public static async Task WriteResultRowsToS3Bucket(this AWSS3API awsS3Api, List<BigQueryRow> rows, BigQueryResults results, EtlSettings etlSettings, string s3Key)
        {
            using (MemoryStream gaStream = new MemoryStream())
            {
                gaStream.WriteGARowsAsParquet(results.Schema, etlSettings.Mappings, rows);
                using (MemoryStream uploadStream = new MemoryStream(gaStream.ToArray()))
                {
                    await awsS3Api.Upload(s3Key, uploadStream);
                }
            }
            rows.Clear();
        }
        
    }
}
