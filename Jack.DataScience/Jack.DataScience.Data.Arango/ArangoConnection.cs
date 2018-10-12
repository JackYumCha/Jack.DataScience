using System;
using ArangoDB.Client;
using System.Net;

namespace Jack.DataScience.Data.Arango
{
    public class ArangoConnection
    {
        private readonly string ArangoID = Guid.NewGuid().ToString();

        public ArangoConnection(ArangoOptions options)
        {
            ArangoDatabase.ChangeSetting(ArangoID, a =>
            {
                a.Url = options.Url;
                a.Credential = options.Credential;
                a.SystemDatabaseCredential = options.SystemCredential;
                a.Database = options.Database;
            });
        }

        public IArangoDatabase CreateClient()
        {
            return ArangoDatabase.CreateWithSetting(ArangoID);
        }
    }

}
