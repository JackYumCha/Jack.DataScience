using Jack.DataScience.Data.Snowflake;
using Jack.DataScience.Storage.AWSS3;
using Jack.DataScience.Storage.AWSS3.Extensions;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Jack.DataScience.Data.SnowflakeEtl
{
    public class SnowflakeEtlAPI
    {
        private readonly SnowflakeAPI snowflakeAPI;
        private readonly AWSS3API awsS3API;
        public SnowflakeEtlAPI(SnowflakeAPI snowflakeAPI, AWSS3API awsS3API)
        {
            this.snowflakeAPI = snowflakeAPI;
            this.awsS3API = awsS3API;
        }

        /// <summary>
        /// execute the snowflake query and save as parquet in s3
        /// </summary>
        /// <param name="query"></param>
        /// <param name="target"></param>
        /// <param name="numberPerFile"></param>
        /// <returns>number of parquet files</returns>
        public async Task<int> QueryAndSave(string query, string target, int numberPerFile)
        {
            var s3Path = target.ParseS3URI();
            var lists = await snowflakeAPI.QueryAsLists(query);
            var headers = lists[0].Cast<string>().ToList();
            var types = lists[1].Cast<Type>().ToList();
            var data = lists.Skip(2).ToList();
            int index = 0;
            string delimiterSlash = s3Path.Key.EndsWith("/") ? "" : "/";
            for (int i = 0; i < data.Count; i+= numberPerFile)
            {
                await awsS3API.WriteParquet(headers, types, data.Skip(i).Take(numberPerFile), 
                    $"{s3Path.Key}{delimiterSlash}{index.ToString().PadLeft(4, '0')}.parquet", s3Path.BucketName);
                index++;
            }
            return index;
        }
    }

}
