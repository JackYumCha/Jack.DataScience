using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Storage.AWSS3
{
    public class AWSS3Options
    {
        public string Key { get; set; }
        public string Secret { get; set; }
        public string Region { get; set; }
        public string Bucket { get; set; }
    }
}
