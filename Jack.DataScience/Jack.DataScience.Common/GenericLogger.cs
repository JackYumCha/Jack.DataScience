using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Common
{
    public class GenericLogger
    {
        public Action<string> Log { get; set; }
        public Action<string> Info { get; set; }
        public Action<string> Error { get; set; }
        public Action<string> Verbose { get; set; }
        public Action<string> Warn { get; set; }
    }
}
