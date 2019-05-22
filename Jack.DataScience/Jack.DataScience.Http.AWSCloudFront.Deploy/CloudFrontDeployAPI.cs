using Jack.DataScience.Storage.AWSS3;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Jack.DataScience.Http.AWSCloudFront.Deploy
{
    public class CloudFrontDeployAPI
    {
        private readonly CloudFrontDeployOptions cloudFrontDeployOptions;
        private readonly AWSS3API awsS3API;
        private readonly AWSCloudFrontAPI awsCloudFrontAPI;
        public CloudFrontDeployAPI(CloudFrontDeployOptions cloudFrontDeployOptions)
        {
            this.cloudFrontDeployOptions = cloudFrontDeployOptions;
            awsS3API = new AWSS3API(cloudFrontDeployOptions.AWSS3Options);
            awsCloudFrontAPI = new AWSCloudFrontAPI(cloudFrontDeployOptions.AWSCloudFrontOptions);
        }

        public async Task<bool> Deploy()
        {
            if (!Directory.Exists(cloudFrontDeployOptions.ArtifactPath)) return false;
            DirectoryInfo artifactRoot = new DirectoryInfo(cloudFrontDeployOptions.ArtifactPath);

            // delete all files in S3 bucket
            var checkHTMLArtifactFile = cloudFrontDeployOptions.DefaultDeleteSafetyCheck;
            if (string.IsNullOrWhiteSpace(checkHTMLArtifactFile))
            {
                checkHTMLArtifactFile = "index.html";
            }

            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"Check for File: {checkHTMLArtifactFile}");
            // avoid deleting the wrong bucket
            if (await awsS3API.FileExists(checkHTMLArtifactFile))
            {
                var allObjects = await awsS3API.ListAllObjectsInBucket();
                Console.ForegroundColor = ConsoleColor.Yellow;
                foreach(var obj in allObjects)
                {
                    await awsS3API.Delete(obj.Key);
                    Console.WriteLine($"Deleted s3://{obj.BucketName}/{obj.Key}");
                }
            }

            // upload all files
            Console.ForegroundColor = ConsoleColor.Green;
            foreach (var file in ListAllFilesInDirectory(artifactRoot))
            {
                using (var fileStream = File.OpenRead($"{artifactRoot.FullName}/{file}"))
                {
                    await awsS3API.UploadAsPublic(file, fileStream);
                    Console.WriteLine($"Uploaded {file}");
                }
            }

            var invalidationId = await awsCloudFrontAPI.CreateInvalidation(cloudFrontDeployOptions.CloudFrontDistributionId);
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine($"CloudFront Invalidation ID: {invalidationId}");
            return true;
        }

        private IEnumerable<string> ListAllFilesInDirectory(DirectoryInfo directoryInfo)
        {
            foreach(var file in directoryInfo.GetFiles())
            {
                yield return file.Name;
            }
            foreach(var directory in directoryInfo.GetDirectories())
            {
                foreach(var file in ListAllFilesInDirectory(directory))
                {
                    yield return $"{directory.Name}/{file}";
                }
            }
        }
    }
}
