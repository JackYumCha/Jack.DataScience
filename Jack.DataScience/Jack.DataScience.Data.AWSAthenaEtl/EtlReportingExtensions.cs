using System;
using System.Text.RegularExpressions;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text;
using MvcAngular;
using Amazon.S3.Model;
using Jack.DataScience.Data.Parquet;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    /// <summary>
    /// read the etl reporting data
    /// </summary>
    public static class EtlReportingExtensions
    {
        public static async Task<EtlReportResponse> GetReports(this EtlSettings etlSettings, EtlReportRequest request)
        {

            var awsS3Api = etlSettings.CreateTargetS3API();

            var paths = await awsS3Api.ListPaths(etlSettings.TargetS3Prefix + "/", "/");

            var dateFrom = DateTime.ParseExact(request.DateFrom, "yyyy-MM-dd", null);
            var dateTo = DateTime.ParseExact(request.DateTo, "yyyy-MM-dd", null);
            var dateIntFrom = int.Parse(dateFrom.ToString("yyyyMMdd"));
            var dateIntTo = int.Parse(dateTo.ToString("yyyyMMdd"));

            Regex dateKeyPathRegex = new Regex(@"^(\d+)\/$");

            var dateKeys = paths.Where(p => dateKeyPathRegex.IsMatch(p))
                .Where(p =>
                {
                    var dateKey = dateKeyPathRegex.Match(p).Groups[1].Value;
                    var dateInt = int.Parse(dateKey);
                    return dateInt >= dateIntFrom && dateInt <= dateIntTo;
                })
                .ToList();

            var allObjectsList = await Task.WhenAll(dateKeys.Select(async dateKey =>
           {
               return await awsS3Api.ListAllObjectsInBucket(prefix: $"{etlSettings.TargetS3Prefix}/{dateKey}");
           }).ToArray());

            var allObjects = allObjectsList.Aggregate(new List<S3Object>(), (seed, list) =>
            {
                if(list != null) seed.AddRange(list);
                return seed;
            });

            var partitionKey = etlSettings.DatePartitionKey;
            var partitionPrefexLength = etlSettings.TargetS3Prefix.Length + 1;
            // read all parquet files

            var allDictLists = await Task.WhenAll(allObjects.Select(async s3Obj =>
           {
               using (var parquetStream = await awsS3Api.OpenReadAsync(s3Obj.Key))
               {
                   var dictList = parquetStream.ReadParquetAdDictData(etlSettings.Mappings.Select(m => m.MappedName).ToList());
                   var relativePath = s3Obj.Key.Substring(partitionPrefexLength);
                   var dateKey = relativePath.Substring(0, relativePath.IndexOf("/"));
                   foreach (var dict in dictList)
                   {
                       dict.Add(partitionKey, dateKey);
                   }
                   return dictList;
               }
           }).ToArray());

            var resultDictData = allDictLists.Aggregate(new List<Dictionary<string, object>>(), (seed, list) =>
            {
                if (list != null) seed.AddRange(list);
                return seed;
            });

            var schema = etlSettings.Mappings.Select(m => m.MappedName).ToList();
            schema.Add(partitionKey);

            return new EtlReportResponse()
            {
                Name = request.Name,
                DateFrom = request.DateFrom,
                DateTo = request.DateTo,
                Schema = schema,
                Data = resultDictData
            };
        }

    }

    [AngularType]
    public class EtlReportRequest
    {
        public string Name { get; set; }
        public string DateFrom { get; set; }
        public string DateTo { get; set; }
    }

    [AngularType]
    public class EtlReportResponse
    {
        public string Name { get; set; }
        public string DateFrom { get; set; }
        public string DateTo { get; set; }
        public List<string> Schema { get; set; }
        public List<Dictionary<string, object>> Data { get; set; }
    }
}
