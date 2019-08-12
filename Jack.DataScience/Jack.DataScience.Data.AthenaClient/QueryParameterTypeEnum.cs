using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.AthenaClient
{
    public enum QueryParameterTypeEnum
    {
        String,
        Integer,
        Double,
        Boolean,
        QueryResult, // will be populated from partitions
        SpecialFormat, // yyyy-mm-dd format for date defined by regex
    }
}
