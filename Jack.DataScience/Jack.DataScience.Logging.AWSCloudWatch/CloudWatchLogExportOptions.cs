using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Logging.AWSCloudWatch
{

    /*
      CREATE EXTERNAL TABLE IF NOT EXISTS cloudwatchlogs.exported_logs (
        `value` string 
      )
      PARTITIONED BY (datekey string, logkey string)
      ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\0'
        ESCAPED BY '\0'
        LINES TERMINATED BY '\n'
      LOCATION 's3://bucket/path/'
      TBLPROPERTIES ('compressionType'='gzip');
    */
    public class CloudWatchLogExportOptions
    {
        public string AthenaTableName { get; set; }
        public List<CloudWatchLogExportEntry> Entries { get; set; }
    }
}
