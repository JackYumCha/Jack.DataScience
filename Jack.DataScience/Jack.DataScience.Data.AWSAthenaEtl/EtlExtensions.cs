using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using System.Collections.Generic;
using Jack.DataScience.Data.CSV;
using Jack.DataScience.Data.Converters;
using Jack.DataScience.Data.Parquet;
using Jack.DataScience.Storage.SFTP;
using Jack.DataScience.Storage.AWSS3;
using Renci.SshNet;
using Renci.SshNet.Sftp;

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
                        var sftpClient = new SftpClient(sftp.Host, sftp.Username, sftp.Password);
                        var files = sftpClient.ListDirectory(sftp.BasePath);
                        files = files.Where(f => nameRegex.IsMatch(f.FullName)).ToList();
                        var first = files.FirstOrDefault();
                        if(first != null)
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
            }

            return etlSettings;
        }

        public static void ReadFromCSVFile(this EtlSettings etlSettings, Stream stream, int lines = 20)
        {
            etlSettings.Mappings = new List<FieldMapping>();
            etlSettings.Sample = new DataSample()
            {
                Rows = new List<DataRow>()
            };

            var config = new Configuration()
            {
                Delimiter = etlSettings.CsvSourceOptoins.Delimiter
            };
            using (var streamReader = new StreamReader(stream))
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
                                etlSettings.Mappings.Add(new FieldMapping()
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
                        for(int i = 0; i < numberOfColoumns; i++)
                        {
                            var field = new FieldMapping()
                            {
                                SourceFieldName = $"Col{i}",
                                MappedName = $"Col{i}"
                            };
                            etlSettings.Mappings.Add(field);
                        }
                    }
                    for(int i =0; i < etlSettings.Mappings.Count; i++)
                    {
                        etlSettings.Mappings[i].MappedType = etlSettings.Sample.Rows
                            .Select(row => row.Items.Count > i ? row.Items[i].ToString() : "")
                            .DetectTypeString()
                            .DetectedTypeToAthenaType();
                    }
                }
            }
        }

        public static string ToMappedName(this string name)
        {
            name = name.Replace(" ", "_");
            if(Regex.IsMatch(name, @"^\d"))
            {
                name = "_" + name;
            }
            return name;
        }

        public static AthenaTypeEnum DetectedTypeToAthenaType(this string type)
        {
            switch (type)
            {
                case "string":
                    return AthenaTypeEnum.athena_string;
                case "int":
                    return AthenaTypeEnum.athena_integer;
                case "double":
                    return AthenaTypeEnum.athena_double;
                case "bool":
                    return AthenaTypeEnum.athena_boolean;
            }
            throw new Exception("Unexpected detected type for athena type conversion.");
        }
        
        public static async Task TestTransferData(this EtlSettings etlSettings)
        {
            switch (etlSettings.SourceType)
            {
                case EtlSourceEnum.SFTP:
                    {
                        var sftp = etlSettings.SFTPSource;
                        var nameRegex = new Regex(sftp.PathRegex);
                        var sftpClient = new SftpClient(sftp.Host, sftp.Username, sftp.Password);
                        var files = sftpClient.ListDirectory(sftp.BasePath);
                        files = files.Where(f => nameRegex.IsMatch(f.FullName)).ToList();
                        // find in the target to work out if there is the corresponding parquet file
                        var targetS3 = etlSettings.CreateTargetS3API();
                        SftpFile first = null;
                        foreach(var file in files)
                        {
                            var partialKey = $"{etlSettings.TargetS3Prefix}{file.Name}";
                            if (!await targetS3.FileExists(partialKey))
                            {
                                first = file;
                                break;
                            }
                        }
                        // transfer that file
                        
                    }
                    break;
                case EtlSourceEnum.S3BucketCheck:
                    {
                        
                    }
                    break;
                case EtlSourceEnum.S3BucketEvent:
                    {

                    }
                    break;
            }
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

        public static async Task TransferCsvStream(this EtlSettings etlSettings, Stream csvStream, string filename)
        {
            var config = new Configuration()
            {
                Delimiter = etlSettings.CsvSourceOptoins.Delimiter
            };
            using(var csvStreamReader = new StreamReader(csvStream))
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
                            if(headers.Count < index)
                            {
                                headers.Add($"Col{index - 1}");
                            }
                            row.Add(value);
                        }
                        data.Add(row);
                        if(data.Count >= etlSettings.NumberOfItemsPerParquet)
                        {
                            var s3key = $"{etlSettings.TargetS3Prefix}/{DateTime.UtcNow.ToString("yyyyMMdd")}/{filename}-{parquetIndex.ToString().PadLeft(5, '0')}.parquet";
                            using (var bufferStream = new MemoryStream())
                            {
                                bufferStream.WriteParquet(etlSettings.Mappings.Select(m => m.ToParquetField()).ToList(), data);
                            }
                            data.Clear();
                            parquetIndex++;
                        }
                    }
                    {
                        var s3key = $"{etlSettings.TargetS3Prefix}/{DateTime.UtcNow.ToString("yyyyMMdd")}/{filename}-{parquetIndex.ToString().PadLeft(5, '0')}.parquet";
                        using (var bufferStream = new MemoryStream())
                        {
                            bufferStream.WriteParquet(etlSettings.Mappings.Select(m => m.ToParquetField()).ToList(), data);
                        }
                        data.Clear();
                        parquetIndex++;
                    }
                    {
                        // upload the flag file
                        var s3key = etlSettings.TargetFlagFile(filename);
                        await targetS3.Upload(s3key, new MemoryStream(Encoding.UTF8.GetBytes("OK")));
                    }
                }
            }
        }

        public static string TargetFlagFile(this EtlSettings etlSettings, string filename)
        {
            return $"{etlSettings.TargetS3Prefix}/{filename}";
        }
    }
}
