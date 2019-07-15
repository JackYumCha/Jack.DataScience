using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Scrapping
{
    public class BrowserScripts
    {
        public Dictionary<string, List<BrowserOperation>> Scripts { get; set; }
        public List<string> Run { get; set; }
        public Dictionary<string, string> Data { get; set; }
    }
}
