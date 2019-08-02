using Autofac;
using Jack.DataScience.Storage.AWSS3;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Jack.DataScience.Scrapping
{
    public static class FileAccessExtensions
    {
        public static void SaveJson<T>(this string path, T obj, IComponentContext componentContext, JsonSerializerSettings jsonSerializerSettings = null)
        {

            var s3Obj = path.ParseS3URI();
            if(s3Obj != null)
            {
                var awsS3API =  componentContext.Resolve<AWSS3API>();
                awsS3API.UploadAsJson(s3Obj.Key, obj, s3Obj.BucketName, jsonSerializerSettings).Wait();
            }
            else
            {
                if (path.StartsWith(".")) path = $"{AppContext.BaseDirectory}/{path}";
                var json = JsonConvert.SerializeObject(obj, jsonSerializerSettings);
                File.WriteAllText(path, json);
            }
        }

        public static void SaveString(this string path, string value, IComponentContext componentContext)
        {
            var s3Obj = path.ParseS3URI();
            if (s3Obj != null)
            {
                var awsS3API = componentContext.Resolve<AWSS3API>();
                using(MemoryStream memory = new MemoryStream(Encoding.UTF8.GetBytes(value)))
                {
                    awsS3API.Upload(s3Obj.Key, memory, s3Obj.BucketName).Wait();
                }
            }
            else
            {
                if (path.StartsWith(".")) path = $"{AppContext.BaseDirectory}/{path}";
                File.WriteAllText(path, value);
            }
        }
        public static void SaveBytes(this string path, byte[] value, IComponentContext componentContext)
        {
            var s3Obj = path.ParseS3URI();
            if (s3Obj != null)
            {
                var awsS3API = componentContext.Resolve<AWSS3API>();
                using (MemoryStream memory = new MemoryStream(value))
                {
                    awsS3API.Upload(s3Obj.Key, memory, s3Obj.BucketName).Wait();
                }
            }
            else
            {
                if (path.StartsWith(".")) path = $"{AppContext.BaseDirectory}/{path}";
                File.WriteAllBytes(path, value);
            }
        }

        public static T ReadJson<T>(this string path, IComponentContext componentContext, JsonSerializerSettings jsonSerializerSettings = null) where T: class, new()
        {
            var s3Obj = path.ParseS3URI();
            if (s3Obj != null)
            {
                var awsS3API = componentContext.Resolve<AWSS3API>();
                return awsS3API.ReadFromJson<T>(s3Obj.Key, s3Obj.BucketName, jsonSerializerSettings).GetAwaiter().GetResult();
            }
            else
            {
                if (path.StartsWith(".")) path = $"{AppContext.BaseDirectory}/{path}";
                var json = File.ReadAllText(path);
                return JsonConvert.DeserializeObject<T>(json, jsonSerializerSettings);
            }
        }
    }
}
