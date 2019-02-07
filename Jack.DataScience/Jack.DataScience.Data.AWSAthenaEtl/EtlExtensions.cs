using System;
using System.IO.Compression;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using System.Collections.Generic;
using Jack.DataScience.Common;
using Jack.DataScience.Data.CSV;
using Jack.DataScience.Data.Converters;
using Jack.DataScience.Data.Parquet;
using Jack.DataScience.Storage.SFTP;
using Jack.DataScience.Storage.AWSS3;
using Jack.DataScience.Data.AWSAthena;
using Renci.SshNet;
using Renci.SshNet.Sftp;
using Newtonsoft.Json;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    public static class EtlExtensions
    {
        public static async Task<EtlSettings> ReadEtlSampleData(this EtlSettings etlSettings, int lines = 20)
        {
            etlSettings.Sample = new DataSample();

            switch (etlSettings.SourceType)
            {
                case EtlSourceEnum.SFTP:
                    {
                        var sftp = etlSettings.SFTPSource;
                        var nameRegex = new Regex(sftp.PathRegex);
                        using (var sftpClient = new SftpClient(sftp.Host, sftp.Username, sftp.Password))
                        {
                            sftpClient.Connect();
                            var files = sftpClient.ListDirectory(sftp.BasePath);
                            files = files.Where(f => nameRegex.IsMatch(f.FullName)).ToList();
                            var first = files.FirstOrDefault();
                            if (first != null)
                            {
                                switch (etlSettings.FileType)
                                {
                                    case EtlFileType.CSV:
                                        {
                                            using (var sftpStream = sftpClient.OpenRead(first.FullName))
                                            {
                                                etlSettings.ReadFromCSVFile(sftpStream, lines);
                                            }
                                        }
                                        break;
                                }
                            }
                            sftpClient.Disconnect();
                        }
                    }
                    break;
                case EtlSourceEnum.S3BucketCheck:
                    {
                        var s3 = etlSettings.S3CheckSource;
                        var awsS3API = new AWSS3API(new AWSS3Options()
                        {
                            Key = s3.Key,
                            Secret = s3.Secret,
                            Bucket = s3.BucketName,
                            Region = s3.Region,
                        });
                        var objects = await awsS3API.ListAllObjectsInBucket(s3.BucketName, s3.Prefix);
                        var nameRegex = new Regex(s3.PathRegex);
                        objects = objects.Where(f => nameRegex.IsMatch(f.Key)).ToList();
                        var first = objects.FirstOrDefault();
                        if (first != null)
                        {
                            switch (etlSettings.FileType)
                            {
                                case EtlFileType.CSV:
                                    {

                                        using (var s3Stream = await awsS3API.OpenReadAsync(first.Key, first.BucketName))
                                        {
                                            etlSettings.ReadFromCSVFile(s3Stream, lines);
                                        }
                                    }
                                    break;
                            }
                        }
                    }
                    break;
                case EtlSourceEnum.S3BucketEvent:
                    {
                        var s3 = etlSettings.S3EventSource;
                        var awsS3API = new AWSS3API(new AWSS3Options()
                        {
                            Key = s3.Key,
                            Secret = s3.Secret,
                            Bucket = s3.BucketName,
                            Region = s3.Region,
                        });
                        if (await awsS3API.FileExists(s3.ExamplePath, s3.BucketName))
                        {
                            switch (etlSettings.FileType)
                            {
                                case EtlFileType.CSV:
                                    {
                                        using (var s3Stream = await awsS3API.OpenReadAsync(s3.ExamplePath, s3.BucketName))
                                        {
                                            etlSettings.ReadFromCSVFile(s3Stream, lines);
                                        }
                                    }
                                    break;
                            }
                        }
                    }
                    break;
                case EtlSourceEnum.GoogleAnalytics:
                    {
                        await etlSettings.GetBigQueryResultSampleByDate(lines);
                    }
                    break;
                case EtlSourceEnum.AmazonAthena:
                    {
                        await etlSettings.GetAthenaQueryResultSampleByDate(lines);
                    }
                    break;
            }

            // make the sample data smaller
            foreach(var row in etlSettings.Sample.Rows.ToList())
            {
                row.Items = row.Items.Select(item => item.Length < 100 ? item : item.Substring(0, 50) + "..." + item.Substring(item.Length - 50)).ToList();
            }

            return etlSettings;
        }




        public static async Task<List<string>> TransferData(this EtlSettings etlSettings, AWSAthenaAPI awsAthenaAPI, GenericLogger logger = null)
        {
            var result = new List<string>();

            logger?.Log?.Invoke($"ETL Mode: {etlSettings.SourceType}");

            switch (etlSettings.SourceType)
            {
                case EtlSourceEnum.SFTP:
                    {
                        var sftp = etlSettings.SFTPSource;
                        var nameRegex = new Regex(sftp.PathRegex);
                        var dateRegex = new Regex(sftp.DateKeyRegex);
                        using (var sftpClient = new SftpClient(sftp.Host, sftp.Username, sftp.Password))
                        {
                            sftpClient.Connect();
                            var files = sftpClient.ListDirectory(sftp.BasePath);
                            files = files.Where(f => nameRegex.IsMatch(f.FullName) && dateRegex.IsMatch(f.Name)).ToList();
                            // find in the target to work out if there is the corresponding parquet file
                            var targetS3 = etlSettings.CreateTargetS3API();
                            SftpFile first = null;
                            foreach (var file in files)
                            {
                                var s3Key = etlSettings.TargetFlagFile(file.Name);
                                if (!await targetS3.FileExists(s3Key))
                                {
                                    first = file;
                                    break;
                                }
                            }
                            // transfer that file
                            if (first != null)
                            {
                                var dateKey = first.Name.MakeRegexExtraction(dateRegex);
                                using (var sftpStream = sftpClient.OpenRead(first.FullName))
                                {
                                    result = await etlSettings.TransferCsvStream(awsAthenaAPI, sftpStream, dateKey, first.Name, false);
                                }
                            }
                            sftpClient.Disconnect();
                        }
                         
                    }
                    break;
                case EtlSourceEnum.S3BucketCheck:
                    {
                        
                    }
                    break;
                case EtlSourceEnum.S3BucketEvent:
                    {
                        var sourceAwsS3Api = new AWSS3API(new AWSS3Options()
                        {
                            Key = etlSettings.S3EventSource.Key,
                            Secret = etlSettings.S3EventSource.Secret,
                            Bucket = etlSettings.S3EventSource.BucketName,
                            Region = etlSettings.S3EventSource.Region
                        });
                        var s3Event = etlSettings.S3EventSource;
                        var nameRegex = new Regex(s3Event.PathRegex);
                        var keyRegex = new Regex(s3Event.FileNameRegex);
                        // do nothing if it does not match the path pattern
                        if (!nameRegex.IsMatch(s3Event.ExamplePath) || (!keyRegex.IsMatch(s3Event.ExamplePath))) return result;

                        // generate dateKey
                        var dateKey = DateTime.UtcNow.ToString("yyyyMMdd");
                        
                        Regex dateRegex = null;
                        if (!s3Event.UseEventDateAsDateKey)
                        {
                            dateRegex = new Regex(s3Event.DateKeyRegex);
                            if (!dateRegex.IsMatch(s3Event.ExamplePath)) return result;
                            dateKey = s3Event.ExamplePath.MakeRegexExtraction(dateRegex);
                        }

                        // generate file name

                        var filename = s3Event.ExamplePath.MakeRegexExtraction(keyRegex);

                        // it will overwrite by default we need to workout datekey first of all
                        var prefixUpToDate = etlSettings.MakeTargetS3Prefix(dateKey, filename, true);

                        // check files that should be deleted
                        var targetAwsS3Api = etlSettings.CreateTargetS3API();
                        var oldObjects = await targetAwsS3Api.ListAllObjectsInBucket(prefix: prefixUpToDate);

                        // delete the files with those prefix
                        foreach(var oldObj in oldObjects)
                        {
                            await targetAwsS3Api.Delete(oldObj.Key);
                        }

                        // open file stream and transfer data
                        using(var awsS3Stream = await sourceAwsS3Api.OpenReadAsync(s3Event.ExamplePath))
                        {
                            result = await etlSettings.TransferCsvStream(awsAthenaAPI, awsS3Stream, dateKey, filename, true);
                        }
                    }
                    break;
                case EtlSourceEnum.GoogleAnalytics:
                    {
                        result = await etlSettings.TransferBigQueryResultByDate(awsAthenaAPI);
                    }
                    break;
                case EtlSourceEnum.AmazonAthena:
                    {
                        result = await etlSettings.TransferAthenaQueryResultByDate(awsAthenaAPI);
                    }
                    break;
            }
            return result;
        }

        public static async Task<List<string>> LoadAllPartitions(this EtlSettings etlSettings, AWSAthenaAPI awsAthenaAPI)
        {
            var results = new List<string>();
            var targetS3Api = etlSettings.CreateTargetS3API();

            var allPaths = await targetS3Api.ListPaths($"{etlSettings.TargetS3Prefix}/");

            foreach(var path in allPaths)
            {
                var dateKey = path.Replace("/", "");
                await awsAthenaAPI.LoadPartition(
                    $"`{etlSettings.AthenaDatabaseName}`.`{etlSettings.AthenaTableName}`",
                    $"`{etlSettings.DatePartitionKey}` = '{dateKey}'",
                    $"s3://{etlSettings.TargetS3BucketName}/{etlSettings.TargetS3Prefix}/{dateKey}/");
                results.Add($"s3://{etlSettings.TargetS3BucketName}/{etlSettings.TargetS3Prefix}/{dateKey}/");
            }
            return results;
        }

        public static AWSS3API CreateTargetS3API(this EtlSettings etlSettings)
        {
            return new AWSS3API(new AWSS3Options()
            {
                Key = etlSettings.TargetAWSKey,
                Secret = etlSettings.TargetAWSSecret,
                Region = etlSettings.TargetS3Region,
                Bucket = etlSettings.TargetS3BucketName
            });
        }


        public static void ReadFromCSVFile(this EtlSettings etlSettings, Stream stream, int lines = 20)
        {
            var newMapptings = new List<FieldMapping>();
            etlSettings.Sample = new DataSample()
            {
                Rows = new List<DataRow>()
            };

            var config = new Configuration()
            {
                Delimiter = etlSettings.CsvSourceOptoins.Delimiter
            };

            var csvStream = stream;

            if (etlSettings.CsvSourceOptoins.GZip)
            {
                csvStream = new GZipStream(stream, CompressionMode.Decompress);
            }

            using (var streamReader = new StreamReader(csvStream))
            {
                using (var csvReader = new CsvReader(streamReader, config))
                {
                    int numberOfColoumns = 0;
                    if (etlSettings.HasHeader)
                    {
                        if (csvReader.Read())
                        {
                            var value = "";
                            int i = 0;
                            while (csvReader.TryGetField(i, out value))
                            {
                                newMapptings.Add(new FieldMapping()
                                {
                                    SourceFieldName = value,
                                    MappedName = value.ToMappedName()
                                });
                                i++;
                            }
                            numberOfColoumns = i;
                        }
                    }
                    int rowCount = 0;
                    while (csvReader.Read() && rowCount < lines)
                    {
                        var value = "";
                        int i = 0;
                        var row = new List<string>();
                        while (csvReader.TryGetField(i, out value))
                        {
                            row.Add(value);
                            i++;
                        }
                        numberOfColoumns = numberOfColoumns <= i ? numberOfColoumns : i;
                        etlSettings.Sample.Rows.Add(new DataRow()
                        {
                            Items = row
                        });
                        rowCount++;
                    }
                    if (!etlSettings.HasHeader)
                    {
                        for (int i = 0; i < numberOfColoumns; i++)
                        {
                            var field = new FieldMapping()
                            {
                                SourceFieldName = $"Col{i}",
                                MappedName = $"Col{i}"
                            };
                            newMapptings.Add(field);
                        }
                    }
                    for (int i = 0; i < newMapptings.Count; i++)
                    {
                        newMapptings[i].MappedType = etlSettings.Sample.Rows
                            .Select(row => row.Items.Count > i ? row.Items[i].ToString() : "")
                            .DetectTypeString()
                            .DetectedTypeToAthenaType();
                    }

                    // update the mappings

                    if (etlSettings.Mappings != null && etlSettings.Mappings.Count > 0)
                    {
                        var oldMappings = etlSettings.Mappings;
                        etlSettings.Mappings = new List<FieldMapping>();
                        for (int i = 0; i < newMapptings.Count; i++)
                        {
                            if (oldMappings.Count > i && oldMappings[i].SourceFieldName == newMapptings[i].SourceFieldName)
                            {
                                etlSettings.Mappings.Add(oldMappings[i]);
                            }
                            else
                            {
                                etlSettings.Mappings.Add(newMapptings[i]);
                            }
                        }
                    }
                    else
                    {
                        etlSettings.Mappings = newMapptings;
                    }
                }
            }
        }

        public static string ToMappedName(this string name)
        {
            name = name.Replace(" ", "_");
            if (Regex.IsMatch(name, @"^\d"))
            {
                name = "_" + name;
            }
            return name;
        }


        public static async Task UpdateS3EventEtlList(this EtlSettings etlSettings, AWSS3API awsS3API, string listKey)
        {


            var list = new List<S3EventHandler>();

            if (await awsS3API.FileExists(listKey))
            {
                try
                {
                    var json = await awsS3API.ReadAsString(listKey);
                    list = JsonConvert.DeserializeObject<List<S3EventHandler>>(json);
                }
                catch (Exception ex)
                {
                    list = new List<S3EventHandler>();
                }
            }

            if (etlSettings.SourceType == EtlSourceEnum.S3BucketEvent)
            {
                // find the key and update
                var found = list.FirstOrDefault(handler => handler.EtlName == etlSettings.Name);
                if (found == null)
                {
                    list.Add(new S3EventHandler()
                    {
                        EtlName = etlSettings.Name,
                        BucketName = etlSettings.S3EventSource.BucketName,
                        PathRegex = etlSettings.S3EventSource.PathRegex
                    });
                }
                else
                {
                    found.BucketName = etlSettings.S3EventSource.BucketName;
                    found.PathRegex = etlSettings.S3EventSource.PathRegex;
                }
            }
            else
            {
                // remove any entries that match that name
                list = list.Where(handler => handler.EtlName != etlSettings.Name).ToList();
            }
          
            // write back to s3
            await awsS3API.UploadAsJson(listKey, list);
        }

        public static async Task DeleteFromS3EventEtlList(this EtlSettings etlSettings, AWSS3API awsS3API, string listKey)
        {
            if (etlSettings.SourceType == EtlSourceEnum.S3BucketEvent)
            {
                var list = new List<S3EventHandler>();

                if (await awsS3API.FileExists(listKey))
                {
                    try
                    {
                        var json = await awsS3API.ReadAsString(listKey);
                        list = JsonConvert.DeserializeObject<List<S3EventHandler>>(json);
                    }
                    catch (Exception ex)
                    {
                        list = new List<S3EventHandler>();
                    }
                }

                // remove any entries that match that name
                list = list.Where(handler => handler.EtlName != etlSettings.Name).ToList();

                // write back to s3
                await awsS3API.UploadAsJson(listKey, list);
            }
        }

        /// <summary>
        /// Process S3 Etl Event
        /// </summary>
        /// <param name="reportingAwsS3Api">The S3 bucket access for the reporting settings</param>
        /// <param name="listKey">s3 event list handler json key</param>
        /// <param name="etlPrefix">etl settings prefex</param>
        /// <param name="awsAthenaAPI">The target athena</param>
        /// <param name="bucketName">event source bucket name</param>
        /// <param name="s3FileKey">event source s3 key</param>
        /// <returns></returns>
        public static async Task<string> ProcessS3EtlEvent(this AWSS3API reportingAwsS3Api, string listKey, string etlPrefix, 
            AWSAthenaAPI awsAthenaAPI, string bucketName, string s3FileKey, GenericLogger logger = null)
        {
            if (!await reportingAwsS3Api.FileExists(listKey)) return $"event handler setting does not exist: '{listKey}'";

            var json = await reportingAwsS3Api.ReadAsString(listKey);
            logger?.Log?.Invoke(json);
            var list = JsonConvert.DeserializeObject<List<S3EventHandler>>(json);

            var found = list.FirstOrDefault(handler => handler.BucketName == bucketName && Regex.IsMatch(s3FileKey, handler.PathRegex));

            if (found == null) return $"event handler not found for object: 's3://{bucketName}/{s3FileKey}'";

            var etlkey = $"{etlPrefix}{found.EtlName}.json";
            logger?.Log?.Invoke($"Find ETL setting: {etlkey}");
            if (! await reportingAwsS3Api.FileExists(etlkey)) return $"etl setting does not exist: '{etlkey}'"; ;

            var jsonEtl = await reportingAwsS3Api.ReadAsString(etlkey);

            var etlSettings = JsonConvert.DeserializeObject<EtlSettings>(jsonEtl);

            // assign the s3FileKey to the ExamplePath and tell people around the deal
            etlSettings.S3EventSource.ExamplePath = s3FileKey;

            var results = await etlSettings.TransferData(awsAthenaAPI);

            return string.Join("\n", results);
        }

        public static async Task<List<string>> TransferCsvStream(this EtlSettings etlSettings, AWSAthenaAPI awsAthenaAPI, Stream stream, string dateKey, string filename, bool keepOriginalName)
        {
            var result = new List<string>();
            var config = new Configuration()
            {
                Delimiter = etlSettings.CsvSourceOptoins.Delimiter
            };

            var csvStream = stream;

            if (etlSettings.CsvSourceOptoins.GZip)
            {
                csvStream = new GZipStream(stream, CompressionMode.Decompress);
            }

            using (var csvStreamReader = new StreamReader(csvStream))
            {
                using (var csvReader = new CsvReader(csvStreamReader, config))
                {
                    var headers = new List<string>();
                    int parquetIndex = 0;

                    var targetS3 = etlSettings.CreateTargetS3API();

                    if (etlSettings.HasHeader)
                    {
                        csvReader.Read();
                        string header = null;
                        int index = 0;
                        while(csvReader.TryGetField(index, out header))
                        {
                            headers.Add(header);
                            index++;
                        }
                    }
                    var mappings = etlSettings.Mappings.ToDictionary(m => m.SourceFieldName, m => m);
                    List<List<string>> data = new List<List<string>>();
                    while (csvReader.Read())
                    {
                        int index = 0;
                        string value = null;
                        var row = new List<string>();
                        while (csvReader.TryGetField(index, out value))
                        {
                            if(headers.Count == index)
                            {
                                headers.Add($"Col{index}");
                            }
                            row.Add(value);
                            index++;
                        }
                        data.Add(row);
                        if(data.Count >= etlSettings.NumberOfItemsPerParquet)
                        {
                            var s3key = etlSettings.MakeTargetS3Key(dateKey, filename, keepOriginalName, parquetIndex);
                            using (var bufferStream = new MemoryStream())
                            {
                                bufferStream.WriteParquet(etlSettings.Mappings.Select(m => m.ToParquetField()).ToList(), data);
                                await targetS3.Upload(s3key, new MemoryStream(bufferStream.ToArray()));
                            }
                            data.Clear();
                            result.Add($"s3://{etlSettings.TargetS3BucketName}/{s3key}");
                            parquetIndex++;
                        }
                    }
                    {
                        var s3key = etlSettings.MakeTargetS3Key(dateKey, filename, keepOriginalName, parquetIndex);
                        using (var bufferStream = new MemoryStream())
                        {
                            bufferStream.WriteParquet(etlSettings.Mappings.Select(m => m.ToParquetField()).ToList(), data);
                            await targetS3.Upload(s3key, new MemoryStream(bufferStream.ToArray()));
                        }
                        data.Clear();
                        result.Add($"s3://{etlSettings.TargetS3BucketName}/{s3key}");
                        parquetIndex++;
                    }
                    {
                        // load partition to athena table
                        await awsAthenaAPI.LoadPartition(
                            $"`{etlSettings.AthenaDatabaseName}`.`{etlSettings.AthenaTableName}`", 
                            $"`{etlSettings.DatePartitionKey}` = '{dateKey}'",
                            $"s3://{etlSettings.TargetS3BucketName}/{etlSettings.TargetS3Prefix}/{dateKey}/");
                    }
                    {
                        // upload the flag file
                        var s3key = etlSettings.TargetFlagFile(filename);
                        await targetS3.Upload(s3key, new MemoryStream(Encoding.UTF8.GetBytes("OK")));
                        result.Add($"s3://{etlSettings.TargetS3BucketName}/{s3key}");
                    }

                }
            }
            return result;
        }


        public static string MakeTargetS3Key(this EtlSettings etlSettings, string dateKey, string filename, bool keepOriginalName, int parquetIndex)
        {
            return $"{etlSettings.MakeTargetS3Prefix(dateKey, filename, keepOriginalName)}{parquetIndex.ToString().PadLeft(5, '0')}.parquet";
        }

        public static string MakeTargetS3Prefix(this EtlSettings etlSettings, string dateKey, string filename, bool keepOriginalName)
        {
            return $"{etlSettings.TargetS3Prefix}/{dateKey}/{(keepOriginalName ? filename + "/" : "")}";
        }

        public static string TargetFlagFile(this EtlSettings etlSettings, string filename)
        {
            return $"{etlSettings.TargetS3Prefix}/{filename}";
        }



   
    }


    
}
