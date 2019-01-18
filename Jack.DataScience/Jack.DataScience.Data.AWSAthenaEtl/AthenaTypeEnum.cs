using MvcAngular;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    [AngularType]
    public enum AthenaTypeEnum
    {
        athena_string,
        athena_varchar,
        athena_tinyint,
        athena_smallint,
        athena_integer,
        athena_bigint,
        athena_double,
        athena_boolean,
        athena_date,
        athena_timestamp
    }
}
