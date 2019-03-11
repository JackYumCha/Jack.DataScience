using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Storage.AWSS3.Extensions
{
    public class S3Db
    {
        public S3Db()
        {

        }

        // generate api tests // tests 

    }

    [AttributeUsage(AttributeTargets.Property)]
    public class S3KeyAttribute: Attribute
    {
        public S3KeyAttribute(int index)
        {
            Index = index;
        }

        public int Index { get; }
    }
}
