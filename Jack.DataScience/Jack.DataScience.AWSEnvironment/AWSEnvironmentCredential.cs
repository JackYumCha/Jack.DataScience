using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.AWSEnvironment
{
    public class AWSEnvironmentCredential
    {
        public string Access { get; set; }
        public string Secret { get; set; }
        public string Region { get; set; }
        public string Token { get; set; }
        public CredentialModeEnum Mode { get; set; }
    }
}
