using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Npgsql;
using Jack.DataScience.Common;

namespace Jack.DataScience.Data.NpgSQL
{
    public class PostgreSQLBootstrap
    {
        private static Regex regexDatabase = new Regex(@"Database=(\w+);");
        private readonly PostgreSQLOptions postgreSQLOptions;
        public PostgreSQLBootstrap(PostgreSQLOptions postgreSQLOptions)
        {
            this.postgreSQLOptions = postgreSQLOptions;
        }

        public string Database
        {
            get
            {
                Match match = regexDatabase.Match(postgreSQLOptions.ConnectionString);
                if (match.Success)
                    return match.Groups[1].Value;
                else
                    return null;
            }
        }

        public void CreateDatabase()
        {
            string noDatabaseConnectionString = regexDatabase.Replace(postgreSQLOptions.ConnectionString, (match) => "");
            using (NpgsqlConnection conn = new NpgsqlConnection(noDatabaseConnectionString))
            {
                conn.Open();
                new NpgsqlCommand($@"CREATE DATABASE {Database};", conn).ExecuteNonQuery();
                conn.Close();
            }
        }

        public void SetupPostGIS()
        {
            // the following operations must be done when a database is connected
            using (NpgsqlConnection conn = new NpgsqlConnection(postgreSQLOptions.ConnectionString))
            {
                conn.Open();
                new NpgsqlCommand(@"CREATE EXTENSION postgis;
-- Enable Topology
CREATE EXTENSION postgis_topology;
-- Enable PostGIS Advanced 3D
-- and other geoprocessing algorithms
-- sfcgal not available with all distributions
CREATE EXTENSION postgis_sfcgal;
-- fuzzy matching needed for Tiger
CREATE EXTENSION fuzzystrmatch;
-- rule based standardizer
CREATE EXTENSION address_standardizer;
-- example rule data set
CREATE EXTENSION address_standardizer_data_us;
-- Enable US Tiger Geocoder
CREATE EXTENSION postgis_tiger_geocoder;", conn).ExecuteNonQuery();
                conn.Close();
            }
        }
    }
}
