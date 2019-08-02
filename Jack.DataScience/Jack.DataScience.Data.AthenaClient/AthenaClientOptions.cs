using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.AthenaClient
{
    public class AthenaClientOptions
    {
        /// <summary>
        /// if the UI shows the editor
        /// </summary>
        public bool Editor { get; set; }
        public string DataRootPath { get; set; }
    }
}
