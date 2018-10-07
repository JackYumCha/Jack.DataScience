using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Common
{
    public class AzureStorageOptions
    {
        public string ConnectionString { get; set; }
        public string Container { get; set; }
    }

    public class LandingZoneOptions : AzureStorageOptions { }
    public class CuratedZoneOptions : AzureStorageOptions { }
}
