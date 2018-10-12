using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    public class MongoBootstrapOptions
    {
        public string DataBaseDirectory { get; set; }
        public string ServiceName { get; set; }
        public string DisplayName { get; set; }
        public int Port { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
    }
}
