using Jack.DataScience.Data.AthenaClient;
using Jack.DataScience.Data.AWSAthena;
using Jack.DataScience.Storage.AWSS3;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jack.DataScience.Data.AthenaUI
{
    public class DecryptedOptions
    {
        public AWSAthenaOptions AWSAthenaOptions { get; set; }
        public AWSS3Options AWSS3Options { get; set; }
        public AthenaClientOptions AthenaClientOptions { get; set; }
    }
}
