using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Jack.DataScience.Storage.AzureBlobStorage
{
    public static class CloudStorageUtils
    {
        public static int CountEntries(this CloudBlobDirectory cloudBlobDirectory)
        {
            BlobContinuationToken blobContinuationToken = null;
            BlobResultSegment blobResultSegment = null;
            int count = 0;
            do
            {
                blobResultSegment = cloudBlobDirectory.ListBlobsSegmentedAsync(blobContinuationToken).GetAwaiter().GetResult();
                blobContinuationToken = blobResultSegment.ContinuationToken;
                count += blobResultSegment.Results.Count();
            } while (blobContinuationToken != null);
            return count;
        }

        public static int CountEntries(this CloudBlobDirectory cloudBlobDirectory, Predicate<IListBlobItem> predicate)
        {
            BlobContinuationToken blobContinuationToken = null;
            BlobResultSegment blobResultSegment = null;
            int count = 0;
            do
            {
                blobResultSegment = cloudBlobDirectory.ListBlobsSegmentedAsync(blobContinuationToken).GetAwaiter().GetResult();
                blobContinuationToken = blobResultSegment.ContinuationToken;
                count += blobResultSegment.Results.Count(item => predicate(item));
            } while (blobContinuationToken != null);
            return count;
        }

        public static IEnumerable<IListBlobItem> AllListBlobItems(this CloudBlobDirectory cloudBlobDirectory)
        {
            BlobContinuationToken blobContinuationToken = null;
            BlobResultSegment blobResultSegment = null;
            do
            {
                blobResultSegment = cloudBlobDirectory.ListBlobsSegmentedAsync(blobContinuationToken).GetAwaiter().GetResult();
                blobContinuationToken = blobResultSegment.ContinuationToken;
                foreach (var item in blobResultSegment.Results)
                {
                    yield return item;
                }
            } while (blobContinuationToken != null);
        }

        public static IEnumerable<IListBlobItem> ListAllBlobItems(this CloudBlobContainer cloudBlobDirectory)
        {
            BlobContinuationToken blobContinuationToken = null;
            BlobResultSegment blobResultSegment = null;
            do
            {
                blobResultSegment = cloudBlobDirectory.ListBlobsSegmentedAsync(blobContinuationToken).GetAwaiter().GetResult();
                blobContinuationToken = blobResultSegment.ContinuationToken;
                foreach (var item in blobResultSegment.Results)
                {
                    yield return item;
                }
            } while (blobContinuationToken != null);
        }

        public static IEnumerable<IListBlobItem> AllListBlobItems(this CloudBlobDirectory cloudBlobDirectory, Predicate<IListBlobItem> predicate)
        {
            BlobContinuationToken blobContinuationToken = null;
            BlobResultSegment blobResultSegment = null;
            do
            {
                blobResultSegment = cloudBlobDirectory.ListBlobsSegmentedAsync(blobContinuationToken).GetAwaiter().GetResult();
                blobContinuationToken = blobResultSegment.ContinuationToken;
                foreach (var item in blobResultSegment.Results.Where(i => predicate(i)))
                {
                    yield return item;
                }
            } while (blobContinuationToken != null);
        }

        public static IEnumerable<IListBlobItem> AllListBlobItems(this CloudBlobContainer cloudBlobDirectory, Predicate<IListBlobItem> predicate)
        {
            BlobContinuationToken blobContinuationToken = null;
            BlobResultSegment blobResultSegment = null;
            do
            {
                blobResultSegment = cloudBlobDirectory.ListBlobsSegmentedAsync(blobContinuationToken).GetAwaiter().GetResult();
                blobContinuationToken = blobResultSegment.ContinuationToken;
                foreach (var item in blobResultSegment.Results.Where(i => predicate(i)))
                {
                    yield return item;
                }
            } while (blobContinuationToken != null);
        }
    }
}
