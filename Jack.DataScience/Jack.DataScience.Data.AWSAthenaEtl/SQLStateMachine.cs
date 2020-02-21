using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    /// <summary>
    /// define the SQL state machine for aws
    /// </summary>
    public class SQLStateMachine
    {

        public string etl { get; set; }
        public int state { get; set; } // default is 0
        public string script { get; set; }
        /// <summary>
        /// this field should be one of SQLStateMachineActions
        /// </summary>
        public string action { get; set; }
        public Dictionary<string, string> Parameters { get; set; }

        public AthenaInput AthenaInput { get; set; }
        public AthenaCheckResult AthenaResult { get; set; }

    }

    public class SQLStateMachineActions
    {
        /// <summary>
        /// after the state machine just started
        /// </summary>
        public const string Origin = "Origin";
        public const string Athena = "Athena";
        public const string Batch = "Batch";
        public const string End = "End";
        public const string Labmda = "Lambda";
    }

    public class AthenaInput
    {
        public string query { get; set; }
        public string access { get; set; }
        public string secret { get; set; }
        public string output { get; set; }
    }

    public class AthenaCheckResult
    {
        public bool boolValue { get; set; }
        public long intValue { get; set; }
        public string strValue { get; set; }
        public bool success { get; set; }
        public string error { get; set; }
        public string state { get; set; }
    }

    public class BatchParameters
    {
        public string JobName { get; set; }
        public string JobQueue { get; set; }
        public string JobDefinition { get; set; }
        public BatchContainerOverrides ContainerOverrides { get; set; }
    }

    public class BatchContainerOverrides
    {
        public List<string> Command { get; set; }
        public List<Dictionary<string, string>> Environment { get; set; }
    }
}
