using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Scrapping
{
    public class ScriptJobMessage
    {
        public string ReceiptHandle { get; set; }
        public ScriptJob Job { get; set; }
    }
}
