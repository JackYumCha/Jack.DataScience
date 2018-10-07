using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.DataTypes
{
    public enum FieldTypeEnum: int
    {
        Serial,
        Integer,
        BigSerial,
        BigInteger,
        VarChar,
        Text,
        Double,
        Boolean,
        Decimal,
        TimeStamp,
        GeometryPoint,
        GeometryLineString,
        GeometryMultiLineString,
        GeometryPolygon,
        GeometryMultiPolygon,
        GeometryCollection
    }
}
