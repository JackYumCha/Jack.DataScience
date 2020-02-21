using Amazon.Athena;
using Amazon.Athena.Model;
using Amazon.Batch.Model;
using Jack.DataScience.Compute.AWSBatch;
using Jack.DataScience.Data.AWSAthena;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Jack.DataScience.AWSAsync
{
    public class AsyncLogic
    {
        private readonly AWSAthenaAPI awsAthenaAPI;
        private readonly AWSBatchAPI awsBatchAPI;
        public AsyncLogic(AWSAthenaAPI awsAthenaAPI, AWSBatchAPI awsBatchAPI)
        {
            this.awsAthenaAPI = awsAthenaAPI;
            this.awsBatchAPI = awsBatchAPI;
        }

        static Regex rgxType = new Regex(@"^\s*```(athena|batch)");

        public async Task<AsyncJobState> CreateAsyncJobState(AsyncJob job)
        {
            AsyncJobState state = new AsyncJobState()
            {
                success = false,
                wait = job.wait
            };
            if (state.wait <= 0) state.wait = 5;
            switch (job.type.ToLower())
            {
                case "athena":
                    {
                        state.type = "Athena";
                        string sql = job.query;
                        state.id = await awsAthenaAPI.StartQuery(sql);
                        Console.WriteLine($"Athena Query:\n" + sql);
                    }
                    break;
                case "batch":
                    {
                        state.type = "Batch";
                        string json = job.query;
                        BatchPayload payload = JsonConvert.DeserializeObject<BatchPayload>(json);
                        state.id = await awsBatchAPI.SubmitJobAndGetID(payload.name, payload.job, payload.queue, payload.parameters);
                        Console.WriteLine($"Batch Job(Name -> {payload.name}, Job -> {payload.job}, Queue -> {payload.queue}\n" + JsonConvert.SerializeObject(payload.parameters));
                    }
                    break;
            }
            return state;
        }

        public async Task<AsyncJobState> CheckAsyncJobState(AsyncJobState state)
        {
            Console.WriteLine($"CheckAsyncJob: {state.type}");
            switch (state.type.ToLower())
            {
                case "athena":
                    {
                        var client = awsAthenaAPI.AthenaClient;
                        var response = await client.GetQueryExecutionAsync(new GetQueryExecutionRequest()
                        {
                            QueryExecutionId = state.id
                        });
                        var executionState = response.QueryExecution.Status.State;
                        state.state = response.QueryExecution.Status.State.Value;
                        state.error = response.QueryExecution.Status.StateChangeReason;
                        if (executionState == QueryExecutionState.SUCCEEDED)
                        {
                            state.success = true;
                            var queryResult = await client.GetQueryResultsAsync(new GetQueryResultsRequest()
                            {
                                QueryExecutionId = state.id,
                                MaxResults = 2
                            });
                            if (queryResult.ResultSet.Rows.Count > 1)
                            {
                                var cell = queryResult.ResultSet.Rows[1].Data[0].VarCharValue;
                                long intValue = 0L;
                                if (long.TryParse(cell, out intValue))
                                {
                                    state.intValue = intValue;
                                }
                                if (!string.IsNullOrWhiteSpace(cell))
                                {
                                    string boolValue = cell.ToLower();
                                    state.boolValue = (boolValue != "false");
                                }
                                state.strValue = cell;
                                Console.WriteLine($"Query: {response.QueryExecution.Query}");
                                Console.WriteLine($"intValue: {state.intValue}, boolValue: {state.boolValue}, strValue: {state.strValue}");
                            }
                        }
                    }
                    break;
                case "batch":
                    {
                        var client = awsBatchAPI.BatchClient;
                        var response = await client.DescribeJobsAsync(new DescribeJobsRequest()
                        {
                            Jobs = new List<string>() { state.id }
                        });
                        var jobDetail = response.Jobs[0];
                        state.error = jobDetail.StatusReason;
                        state.state = jobDetail.Status.Value;
                        switch (state.state)
                        {
                            case "SUCCEEDED":
                                state.success = true;
                                break;
                            case "FAILED":
                                state.success = false;
                                break;
                            case "SUBMITTED":
                            case "PENDING":
                            case "RUNNABLE":
                            case "RUNNING":
                                state.state = "RUNNING";
                                state.success = false;
                                break;
                        }
                    }
                    break;
            }
            Console.WriteLine($"StatusReason: {state.error}");
            Console.WriteLine($"State: {state.state}");
            return state;
        }
    }
}
