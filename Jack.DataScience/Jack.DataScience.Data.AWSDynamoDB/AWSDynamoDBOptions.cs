﻿using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.AWSDynamoDB
{
    public class AWSDynamoDBOptions
    {
        public string Key { get; set; }
        public string Secret { get; set; }
        public string Region { get; set; }
        public string TableName { get; set; }
    }
}
