using System;
using System.Collections.Generic;
using System.Security.Authentication;
using System.Text;

namespace Jack.DataScience.LogWrapper
{
    public class LogWrapperOptions
    {
        public string MongoDBUrl { get; set; }
        public string Database { get; set; }
        public SslProtocols SslProtocol { get; set; }
        public string CollectionName { get; set; }
        public string Command { get; set; }
        public List<string> Arguments { get; set; }
    }
}
