using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Storage.SFTP
{
    public class LocalPortEntry
    {
        public string Local { get; set; }
        public uint LocalPort { get; set; }
        public string Remote { get; set; }
        public uint RemotePort { get; set; }
    }
}
