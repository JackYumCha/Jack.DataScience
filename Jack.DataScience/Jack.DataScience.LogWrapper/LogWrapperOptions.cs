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
        public bool Capped { get; set; }
        public int MaxDocuments { get; set; }
        public long MaxSize { get; set; }
        public string Command { get; set; }
        public List<string> Arguments { get; set; }
    }
}
