using System;
using System.Collections.Generic;
using System.Text;


namespace Jack.DataScience.Logging.Serilog
{
    public class SerilogOptions
    {
        public string RollingFileLogEventLevel { get; set; }
        public string ConsoleLogEventLevel { get; set; }
    }
}
