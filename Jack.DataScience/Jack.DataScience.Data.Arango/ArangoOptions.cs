using System;
using System.Collections.Generic;
using System.Text;
using System.Net;

namespace Jack.DataScience.Data.Arango
{
    public class ArangoOptions
    {
        public string Url { get; set; }
        public string Database { get; set; }
        public NetworkCredential Credential { get; set; }
        public NetworkCredential SystemCredential { get; set; }
    }
}
