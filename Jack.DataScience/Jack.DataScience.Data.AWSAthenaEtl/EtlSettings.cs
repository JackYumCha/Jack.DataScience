﻿using System;
using System.Collections.Generic;
using System.Text;
using MvcAngular;
using Jack.DataScience.Data.AWSAthena;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    [AngularType]
    public class EtlSettings
    {
        public EtlSourceEnum SourceType { get; set; }
        public SFTPSetting SFTPSource { get; set; }
        public S3BucketCheckSetting S3CheckSource { get; set; }
        public S3BucketEventSetting S3EventSource { get; set; }
        public EtlFileType FileType { get; set; }
        public CsvSourceOptoins CsvSourceOptoins { get; set; }
        public bool HasHeader { get; set; }
        public List<FieldMapping> Mappings { get; set; }
        public string TargetAWSKey { get; set; }
        public string TargetAWSSecret { get; set; }
        public string TargetS3Region { get; set; }
        public string TargetS3BucketName { get; set; }
        public string TargetS3Prefix { get; set; }
        public int NumberOfItemsPerParquet { get; set; }
        public DataSample Sample { get; set; }
    }

    [AngularType]
    public class CsvSourceOptoins
    {
        public string Delimiter { get; set; }
        /// <summary>
        /// by index or by header name
        /// </summary>
        public bool ByColumnIndex { get; set; }
    }

    [AngularType]
    public class FieldMapping
    {
        /// <summary>
        /// when by index, the format is Col0, Col1, etc
        /// </summary>
        public string SourceFieldName { get; set; }
        public string MappedName { get; set; }
        public AthenaTypeEnum MappedType { get; set; }
    }

    public static class FieldMappingExtensions
    {
        public static ParquetField ToParquetField(this FieldMapping fieldMapping)
        {
            return new ParquetField()
            {
                AthenaType = fieldMapping.MappedType,
                Name = fieldMapping.MappedName
            };
        }
    }

    [AngularType]
    public class DataRow
    {
        public List<string> Items { get; set; }
    }

    [AngularType]
    public class DataSample
    {
        public List<DataRow> Rows { get; set; }
    }

    [AngularType]
    public class SFTPSetting
    {
        public string Host { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string BasePath { get; set; }
        public string PathRegex { get; set; }
    }

    [AngularType]
    public class S3BucketCheckSetting
    {
        public string Key { get; set; }
        public string Secret { get; set; }
        public string Region { get; set; }
        public string BucketName { get; set; }
        public string Prefix { get; set; }
        public string PathRegex { get; set; }
        /// <summary>
        /// the processed file will be moved to this location
        /// </summary>
        public string ProcessedPrefix { get; set; }
    }

    [AngularType]
    public class S3BucketEventSetting
    {
        public string Key { get; set; }
        public string Secret { get; set; }
        public string Region { get; set; }
        public string BucketName { get; set; }
        public string ExamplePath { get; set; }
        public string PathRegex { get; set; }
        /// <summary>
        /// the processed file will be moved to this location
        /// </summary>
        public string ProcessedPrefix { get; set; }
    }

    /// <summary>
    /// the etl exception for etl problems
    /// </summary>
    public class EtlException: Exception
    {
        public EtlException(string message) : base(message) { }
    }
}
