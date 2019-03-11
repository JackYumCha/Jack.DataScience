using System;
using System.Collections.Generic;
using System.Text;
using System.Security.Authentication;

namespace Jack.DataScience.Data.MongoDB
{
    public class MongoOptions
    {
        public string Url { get; set; }
        public SslProtocols SslProtocol { get; set; }
    }
}
