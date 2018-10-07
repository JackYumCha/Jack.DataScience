using System;
using System.Collections.Generic;
using System.Text;
using MongoDB.Bson.Serialization.Attributes;
using Jack.DataScience.Data.NpgSQL;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using ArangoDB.Client;

namespace Jack.DataScience.DataTypes
{

    public class DemoEntity: VertexBase
    {
        [BsonId, Key, Required]
        public new string _id { get; set; }

        [Index, StringLength(255)]
        public string Name { get; set; }
        [Index]
        public int Age { get; set; }
        [Index, StringLength(255)]
        public string Email { get; set; }
    }
}
