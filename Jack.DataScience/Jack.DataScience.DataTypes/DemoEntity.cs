using System;
using System.Collections.Generic;
using System.Text;
using MongoDB.Bson.Serialization.Attributes;
using Jack.DataScience.Data.NpgSQL;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Jack.DataScience.DataTypes
{

    class DemoEntity
    {
        [BsonId, Key, Required]
        public string _id { get; set; }
        [Index, StringLength(255)]
        public string Name { get; set; }
        [Index]
        public int Age { get; set; }
        [Index, StringLength(255)]
        public string Email { get; set; }
    }
}
