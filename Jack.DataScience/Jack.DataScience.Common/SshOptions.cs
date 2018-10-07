using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Common
{
    public class SshOptions
    {
        public string Url { get; set; }
        public int Port { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string DefaultPath { get; set; }
    }
}
