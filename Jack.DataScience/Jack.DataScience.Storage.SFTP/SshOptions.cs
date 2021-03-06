﻿using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Storage.SFTP
{
    public class SshOptions
    {
        public string Url { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string DefaultPath { get; set; }
        public string PrivateKeyPath { get; set; }
        public string AuthenticationMethod { get; set; }
    }
}
