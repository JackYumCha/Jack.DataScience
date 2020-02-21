using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Xunit;
using Jack.DataScience.StringCompression;
using System.Linq;

namespace Jack.DataScience.Data.AWSAthenaEtl.Tests
{
    public class StateMachineQueryTests
    {

        [Fact()]
        public async void StateQueryTests()
        {
            int i = 0;
            StateMachineExecutionResult result;
            StateMachineQueryContext context;
            {
                i = 1;
                AthenaParserSetting athenaParserLogger = new AthenaParserSetting();
                Debug.WriteLine($"****** Begin File {i} ******");
                var filename = $"{AppContext.BaseDirectory}/query{i}.sql";
                Debug.WriteLine($"File {i}: {filename}");
                var query = File.ReadAllText(filename);
                try
                {
                    
                    var pipes = query.ParseAthenaPipes(athenaParserLogger);
                    Debug.WriteLine($"****** End File {i} ******");
                    Debug.WriteLine($"****** Json File {i} ******");
                    var tree = JsonConvert.SerializeObject(pipes, Formatting.Indented);
                    Debug.WriteLine($"****** Parsed File {i} ******");

                    bool seeking = false;
                    context = new StateMachineQueryContext();

                    // context.ExecuteStateMachineQueryContext()
                    
                    result = pipes.LoadNextStateMachineQuery(new AthenaParserSetting(), new LinkedList<int>(), context,  ref seeking);
                    seeking = true;
                    result = pipes.LoadNextStateMachineQuery(new AthenaParserSetting(), new LinkedList<int>(), context, ref seeking);
                    Debug.WriteLine(pipes.ToQueryString().StripEmptyLines());
                }
                catch (Exception ex)
                {
                    Debug.WriteLine(athenaParserLogger.ToString());
                    Debug.Write(ex.Message);
                }

            }

            

        }

        [Fact()]
        public void TestStateMachine()
        {

            string cachedJson = File.ReadAllText($"{AppContext.BaseDirectory}/caches.json");
            int i = 4;
            switch (i)
            {
                case 1:
                    {
                        var context = new DateTime(2019, 11, 13).BuildFromFile(i);
                        context.ExecuteStateMachineQueryContext();
                        context.Print();
                        Debugger.Break(); // see how it goes up to here

                        context.result = new StateMachineQueryResult();
                        context.result.SetValue(200);

                        context.ExecuteStateMachineQueryContext();
                        context.Print();
                        Debugger.Break();
                    }
                    break;
                case 2:
                    {
                        var context = new DateTime(2019, 11, 13).BuildFromFile(i);
                        context.RunWithResult(null);
                        context.RunWithResult(StateMachineQueryResult.Void);
                        context.RunWithResult(StateMachineQueryResult.True);
                        context.RunWithResult(StateMachineQueryResult.Void);
                        context.RunWithResult(StateMachineQueryResult.Void);
                        context.RunWithResult(StateMachineQueryResult.False);
                        context.RunWithResult(StateMachineQueryResult.True);
                        context.RunWithResult(StateMachineQueryResult.Void);
                        context.RunWithResult(StateMachineQueryResult.False);
                        context.RunWithResult(StateMachineQueryResult.Void);
                        Debugger.Break();
                    }
                    break;
                case 3:
                    {
                        var context = new DateTime(2019, 11, 13).BuildFromFile(i);
                        context.RunWithResult(null);
                        context.RunWithResult(StateMachineQueryResult.Void);
                        context.RunWithResult(StateMachineQueryResult.False);
                        context.RunWithResult(StateMachineQueryResult.False);
                        context.RunWithResult(StateMachineQueryResult.True);
                        context.RunWithResult(StateMachineQueryResult.Void);
                        Debugger.Break();
                    }
                    break;
                case 4:
                    {
                        var context = new DateTime(2019, 11, 13).BuildFromFile(i);
                        context.RunWithResult(null);
                        context.RunWithResult(StateMachineQueryResult.Integer(1));
                        context.RunWithResult(StateMachineQueryResult.Void);
                        for(int j = 0; j < 6; j++)
                            context.RunWithResult(StateMachineQueryResult.Void);
                        Debugger.Break();
                    }
                    break;
                case 5:
                    {
                        var context = new DateTime(2019, 11, 13).BuildFromFile(i);
                        context.RunWithResult(null);
                        context.RunWithResult(StateMachineQueryResult.Integer(0));
                        context.RunWithResult(StateMachineQueryResult.Void);
                        context.RunWithResult(StateMachineQueryResult.True);
                        context.RunWithResult(StateMachineQueryResult.Void);
                        context.RunWithResult(StateMachineQueryResult.False);
                        Debugger.Break();
                    }
                    break;
            }
            // case try the 
        }


        [Fact()]
        public void StringCompressionTests()
        {
            byte[] data = new byte[] { 221, 123, 243, 8, 193, 25, 136 };

            // 221 % 93
            // 221+123*256+243*256^2+... % 93 
            // = 221 * 1 + 123 * 70 + 243 * 64
            // 

            /*
                1
                70
                64
                16
                4
            */
            
            

            Debugger.Break();


            //int start = 3;
            //int residue = start;
            //List<int> residues = new List<int>();
            //residues.Add(residue);
            //do
            //{
            //    residue = residue * 256;
            //    residue %= 93;
            //    residues.Add(residue);
            //} while (residue != start);

            //Debugger.Break();
            // build char array
            //List<char> charsList = new List<char>();
            //int[] map = new int[127];
            //int i = 0;
            //for (char c = ' '; c < 127; c++)
            //{
            //    if (c == '\\' || c == '"') continue;
            //    charsList.Add(c);
            //    map[c] = i;
            //    i++;
            //}

            //List<string> mapBuilder = new List<string>();
            //for(int j = 0; j < 127; j++)
            //{
            //    mapBuilder.Add($"{map[j]}");
            //}

            //string charArray = string.Join(",", charsList.Select(c => $"'{c}'"));

            //string mapArray = string.Join(",", mapBuilder);

            //Debugger.Break(); 


            string test1 = @"-- if (
select * 
from b.atset
--)
-- {
@export()
select a as name from test
-- }

";
            var jsonValue1 = test1.CompressBase64();

            var value1 = jsonValue1.DecompressBase64();

            Debugger.Break();
        }
    }
}
