using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    public class VertexInfo
    {
        public string _id { get; set; }
        public string collection { get; set; }
        public Direction direction { get; set; }
        public override string ToString()
        {
            return $"{collection}::{_id} ({direction})";
        }
    }
}
