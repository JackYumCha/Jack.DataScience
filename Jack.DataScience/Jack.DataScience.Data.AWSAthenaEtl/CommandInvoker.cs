using Jack.DataScience.Compute.AWSBatch;
using Jack.DataScience.Compute.AWSLambda;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    public static class CommandInvokerExtensions
    {

        //public static async Task Invoke(this StateMachineQueryContext context, AWSBatchAPI awsBatchAPI, AWSLambdaAPI awsLambdaAPI)
        //{
        //    // run batch or lambda
        //    foreach(var command in context.settings.Commands)
        //    {
        //        Match matchCommand;
                
        //        if (AthenaSQLPipeExtensions.rgxBatchCommand.IsMatch(command))
        //        {
        //            matchCommand = AthenaSQLPipeExtensions.rgxBatchCommand.Match(command);
        //            string arnQueue = matchCommand.Groups[1].Value, arnJob = matchCommand.Groups[2].Value, name = matchCommand.Groups[3].Value,
        //                type = matchCommand.Groups[4].Value.ToLower(), json = matchCommand.Groups[5].Value.TrimEnd(new char[] { ' ' });

        //            Console.WriteLine($"Batch Queue = {arnQueue} Job = {arnJob} Name = {name} Type = {type} Value = {json}");

        //            await awsBatchAPI.SubmitJob(name, arnJob, arnQueue, JsonConvert.DeserializeObject<Dictionary<string, string>>(json));
        //        }
        //        else if (AthenaSQLPipeExtensions.rgxLambdaCommand.IsMatch(command))
        //        {
        //            matchCommand = AthenaSQLPipeExtensions.rgxLambdaCommand.Match(command);
        //            string arnFunction = matchCommand.Groups[1].Value,
        //                type = matchCommand.Groups[2].Value.ToLower(), json = matchCommand.Groups[3].Value.TrimEnd(new char[] { ' ' });

        //            Console.WriteLine($"Batch Function = {arnFunction} Type = {type} Value = {json}");

        //            await awsLambdaAPI.Invoke(arnFunction, JsonConvert.DeserializeObject<JObject>(json));
        //        }
        //    }
            
        //}
    }
}
