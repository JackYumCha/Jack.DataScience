using Amazon.S3.Model;
using Autofac;
using Jack.DataScience.Storage.AWSS3;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using OpenQA.Selenium;
using OpenQA.Selenium.Remote;
using OpenQA.Selenium.Chrome;
using OpenQA.Selenium.Firefox;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
//using Console = Jack.DataScience.Common.Logging.Console;
using Jack.DataScience.Compute.AWSEC2;
using System.Threading;

namespace Jack.DataScience.Scrapping
{
    public class ScrapingEngine
    {
        //public const string ChromeDriverPathKey = "CHROME_DRIVER_PATH";
        private IComponentContext componentContext;
        public const string EC2_ID = "EC2_ID";
        public ScrapingEngine() {}

        public ScrapingEngine(IComponentContext componentContext)
        {
            this.componentContext = componentContext;
            TryResolve();
        }

        public void TryResolve()
        {
            ChromeOptions chromeOptions = null;
            if (componentContext.TryResolve(out chromeOptions)) DriverOptions = chromeOptions;
            FirefoxOptions firefoxOptions = null;
            if (componentContext.TryResolve(out firefoxOptions)) DriverOptions = firefoxOptions;
            BrowserScripts scripts = null;
            if (componentContext.TryResolve(out scripts)) Scripts = scripts;
        }

        public BrowserScripts Scripts { get; set; }
        public DriverOptions DriverOptions { get; set; }
        public Dictionary<string, Action<IEnumerable<IWebElement>>> Jobs { get; set; } = new Dictionary<string, Action<IEnumerable<IWebElement>>>();
        public Dictionary<string, List<IWebElement>> References { get; set; } = new Dictionary<string, List<IWebElement>>();
        public Dictionary<string, JObject> Jsons { get; set; } = new Dictionary<string, JObject>();
        public Dictionary<string, BrowserOperation> Functions { get; set; } = new Dictionary<string, BrowserOperation>();

        public void RunTest()
        {
            Console.WriteLine("Test Case with https://www.google.com");

            ChromeOptions chromeOptions = new ChromeOptions();
            chromeOptions.AddArgument("--no-sandbox");
            chromeOptions.AddArgument("--disable-gpu");
            chromeOptions.AddArgument("--headless");
            ChromeDriver chromeDriver = new ChromeDriver(AppContext.BaseDirectory, chromeOptions);

            chromeDriver.Url = "https://www.google.com";
            chromeDriver.GetScreenshot().SaveAsFile("screenshot.png");

            chromeDriver.Dispose();

            Console.WriteLine($"Screenshot saved to screenshot.png");
        }

        public Dictionary<string, string> RunScript(string key, Dictionary<string,string> dynamicData = null, DriverOptions options = null)
        {
            Console.WriteLine($"RunScript({key})");

            if (dynamicData == null) dynamicData = new Dictionary<string, string>();
            if (Scripts == null || Scripts.Scripts == null || !Scripts.Scripts.ContainsKey(key) )
            {
                Console.WriteLine($"Script Key={key} does not exist in the Scripts Data.");
                return dynamicData;
            }

            var operations = Scripts.Scripts[key];

            IWebDriver webDriver = null;

            var browser = "chrome";
            if (dynamicData.ContainsKey("browser"))
            {
                browser = dynamicData["browser"];
            }

            // overwrite default
            if (options == null && DriverOptions != null) options = DriverOptions;

            Console.WriteLine($"Is Linux: {RuntimeInformation.IsOSPlatform(OSPlatform.Linux)}");
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                switch (browser.ToLower())
                {
                    case "chrome":
                        {
                            if (options == null)
                            {
                                var chromeOptions = new ChromeOptions();
                                chromeOptions.AddArgument("--headless");
                                chromeOptions.AddArgument("--disable-gpu");
                                chromeOptions.AddArgument("--no-sandbox");
                                chromeOptions.AddArgument("--window-size=1920,1080");
                                chromeOptions.AddArgument("--ignore-certificate-errors");
                                chromeOptions.AddArgument("--log-level=3");
                                options = chromeOptions;
                            }
                        }
                        break;
                    case "firefox":
                        {
                            if (options == null)
                            {
                                var firefoxOptions = new FirefoxOptions();
                                firefoxOptions.AddArgument("-headless");
                                //firefoxOptions.AddArgument("-log-level=5");
                                firefoxOptions.SetPreference("security.sandbox.content.level", "5");
                                firefoxOptions.SetPreference("browser.tabs.remote.autostart", false);
                                firefoxOptions.SetPreference("browser.tabs.remote.autostart.2", false);
                                options = firefoxOptions;
                            }
                        }
                        break;
                }
             
            }

            if (options == null)
            {
                switch (browser.ToLower())
                {
                    case "chrome":
                        webDriver = new ChromeDriver(AppContext.BaseDirectory);
                        break;
                    case "firefox":
                        webDriver = new FirefoxDriver(AppContext.BaseDirectory);
                        break;
                }
                
            }
            else
            {
                switch (browser.ToLower())
                {
                    case "chrome":
                        webDriver = new ChromeDriver(AppContext.BaseDirectory, options as ChromeOptions);
                        break;
                    case "firefox":
                        webDriver = new FirefoxDriver(AppContext.BaseDirectory, options as FirefoxOptions);
                        break;
                }
            }

            try
            {
                foreach (var operation in operations)
                {
                    operation.RunOperation(webDriver, dynamicData, Jobs, References, Jsons, Functions, 0, componentContext);
                }
                if (dynamicData.ContainsKey("success"))
                {
                    dynamicData["success"] = "true";
                }
                else
                {
                    dynamicData.Add("success", "true");
                }
            }
            catch(Exception ex)
            {
                do
                {
                    Console.Error.WriteLine($"[Exception] {ex.GetType().FullName}");
                    Console.Error.WriteLine($"[Error Message] {ex.Message}");
                    Console.Error.WriteLine($"[Stack Trace]:\n {ex.StackTrace}");
                    ex = ex.InnerException;
                } while (ex != null);
                if (Functions.ContainsKey("error"))
                {
                    Functions["error"].RunDriverThen(webDriver, dynamicData, Jobs, References, Jsons, Functions, 0, componentContext);
                }
                if (dynamicData.ContainsKey("success"))
                {
                    dynamicData["success"] = "false";
                }
                else
                {
                    dynamicData.Add("success", "false");
                }
            }

            //Debugger.Break();

            webDriver.Dispose();
            return dynamicData;
        }

        public async Task<bool> RunQueueJob()
        {
            var scheduler = componentContext.Resolve<ScriptJobScheduler>();
            var s3 = componentContext.Resolve<AWSS3API>();
            var options = componentContext.Resolve<AWSScrapeJobOptions>();

            Func<Task> shutdown = async () =>
            {
                if (options.ShutdownEC2)
                {
                    Console.WriteLine($"Try to Shut down EC2 Instance:");
                    var ec2_id = Environment.GetEnvironmentVariable(EC2_ID);
                    if (!string.IsNullOrEmpty(ec2_id))
                    {
                        var ec2 = componentContext.Resolve<AWSEC2API>();
                        Console.WriteLine($"Shutting down EC2 Instance {ec2_id}...");
                        await ec2.StopByIds(new List<string>() { ec2_id });
                        // wait stop to run
                        Thread.Sleep(3000);
                    }
                    else
                    {
                        Console.WriteLine($"EC2_ID not found in Environment Variable.");
                    }
                }
            };

            do
            {
                ScriptJob job = null;
                ScriptJobMessage message = null;
                S3Object scriptFileObj = null;
                while (job == null)
                {
                    message = await scheduler.GetScriptJob();
                    if (message == null)
                    {
                        Console.WriteLine($"No ScriptJob Found. Exit Scrape Engine.");
                        await shutdown();
                        return false;
                    }

                    // run the job
                    job = message.Job;
                    try
                    {
                        if (options.TestMode)
                        {
                            if (options.TestScriptMapping.ContainsKey(job.Script))
                            {
                                var filename = options.TestScriptMapping[job.Script];
                                if (filename.StartsWith(".")) filename = $"{AppContext.BaseDirectory}/{filename}";
                                if (!File.Exists(filename))
                                {
                                    Console.WriteLine($"Local Mapping File for Script '{job.Script}' Dost Not Exist in File System. Job is about to Fail.");
                                }
                            }
                            else
                            {
                                Console.WriteLine($"No Local Mapping Found for Script '{job.Script}'. Job is about to Fail.");
                                Debugger.Break();
                                await scheduler.CompleteScriptJob(message, false);
                                continue;
                            }
                        }
                        else
                        {
                            scriptFileObj = job.Script.ParseS3URI();
                            if (!await s3.FileExists(scriptFileObj.Key, scriptFileObj.BucketName))
                            {
                                await scheduler.CompleteScriptJob(message, false);
                                continue;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        await scheduler.CompleteScriptJob(message, false);
                        continue;
                    }
                }

                // read the script
                BrowserScripts browserScripts = null;
                if (options.TestMode)
                {
                    var filename = options.TestScriptMapping[job.Script];
                    if (filename.StartsWith(".")) filename = $"{AppContext.BaseDirectory}/{filename}";
                    browserScripts = JsonConvert.DeserializeObject<BrowserScripts>(File.ReadAllText(filename));
                }
                else
                {
                    browserScripts = await s3.ReadFromJson<BrowserScripts>(scriptFileObj.Key, scriptFileObj.BucketName);
                }


                // run each scripts

                Scripts = browserScripts;
                if (browserScripts.Data == null)
                {
                    browserScripts.Data = new Dictionary<string, string>();
                }

                // load the JObject payload data from the messsage
                Jsons = new Dictionary<string, JObject>();
                if (job.Payload != null)
                {
                    foreach (var property in job.Payload.Properties())
                    {
                        if (property.HasValues)
                        {
                            // we will store JObject in Jsons and key-value pairs in Dictionary
                            switch (property.Value.Type)
                            {
                                case JTokenType.Object:
                                    Jsons.Add(property.Name, property.Value<JObject>());
                                    break;
                                case JTokenType.String:
                                    browserScripts.Data.Add(property.Name, property.Value.Value<string>());
                                    break;
                                case JTokenType.Integer:
                                    browserScripts.Data.Add(property.Name, property.Value.Value<string>());
                                    break;
                                case JTokenType.Float:
                                    browserScripts.Data.Add(property.Name, property.Value.Value<string>());
                                    break;
                                case JTokenType.Boolean:
                                    browserScripts.Data.Add(property.Name, property.Value.Value<string>());
                                    break;
                                case JTokenType.Date:
                                    browserScripts.Data.Add(property.Name, property.Value.Value<string>());
                                    break;
                                default:
                                    Console.WriteLine($"Payload Property {property.Name} Ignored. Property type {property.Value.Type} is not supported!");
                                    break;
                            }
                        }
                    }
                }

                foreach (var key in Scripts.Run)
                {
                    if (browserScripts.Data.ContainsKey("success"))
                    {
                        browserScripts.Data["success"] = "false";
                    }
                    else
                    {
                        browserScripts.Data.Add("success", "false");
                    }
                    RunScript(key, browserScripts.Data);
                    switch (browserScripts.Data["success"])
                    {
                        case "true":
                            Console.WriteLine($"Job Succeeded: {job.Script} {job.Job} -> {key}");
                            scheduler.CompleteScriptJob(message, true).Wait();
                            break;
                        case "false":
                            Console.WriteLine($"Job Failed: {job.Script} {job.Job} -> {key}");
                            scheduler.CompleteScriptJob(message, false).Wait();
                            break;
                    }
                }

                if (options.MultipleJobs)
                {
                    Console.WriteLine($"MultipleJobs Mode. Try another job now:");
                }
            } while (options.MultipleJobs);

            await shutdown();

            return true;
        }


        public async Task<bool> DebugScript(string script)
        {
            var scheduler = componentContext.Resolve<ScriptJobScheduler>();
            var jobs = await scheduler.GetAllJobs(script);
            Queue<ScriptJob> queue = new Queue<ScriptJob>();
            Random random = new Random((int)(DateTime.UtcNow - new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds);
            jobs.OrderBy(job => random.NextDouble()).ToList().ForEach(job => queue.Enqueue(job));
            while (queue.Count > 0)
            {
                var job = queue.Dequeue();
                await RunJob(job);
            }
            return true;
        }

        public async Task<bool> DebugScriptJob(string script, string job)
        {
            var scheduler = componentContext.Resolve<ScriptJobScheduler>();
            var item = await scheduler.GetScriptJob(script, job);
            await RunJob(item);
            return true;
        }


        public async Task<bool> DebugRetries(string script)
        {
            var scheduler = componentContext.Resolve<ScriptJobScheduler>();
            var jobs = await scheduler.GetRetryingJobs(script);
            Queue<ScriptJob> queue = new Queue<ScriptJob>();
            Random random = new Random((int)(DateTime.UtcNow - new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds);
            jobs.OrderBy(job => random.NextDouble()).ToList().ForEach(job => queue.Enqueue(job));
            while(queue.Count > 0)
            {
                var job = queue.Dequeue();
                await RunJob(job);
            }
            return true;
        }

        public async Task<bool> DebugFailures(string script)
        {
            var scheduler = componentContext.Resolve<ScriptJobScheduler>();
            var jobs = await scheduler.GetFailedJobs(script);
            Queue<ScriptJob> queue = new Queue<ScriptJob>();
            Random random = new Random((int)(DateTime.UtcNow - new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds);
            jobs.OrderBy(job => random.NextDouble()).ToList().ForEach(job => queue.Enqueue(job));
            while (queue.Count > 0)
            {
                var job = queue.Dequeue();
                await RunJob(job);
            }
            return true;
        }

        public async Task RunJob(ScriptJob job)
        {
            S3Object scriptFileObj = null;

            var scheduler = componentContext.Resolve<ScriptJobScheduler>();
            var s3 = componentContext.Resolve<AWSS3API>();
            var options = componentContext.Resolve<AWSScrapeJobOptions>();

            scriptFileObj = job.Script.ParseS3URI();
            if (!await s3.FileExists(scriptFileObj.Key, scriptFileObj.BucketName))
            {
                throw new ScrapingException($"No Job Script is found in target S3 location");
            }

            BrowserScripts browserScripts = null;
            if (options.TestMode)
            {
                var filename = options.TestScriptMapping[job.Script];
                if (filename.StartsWith(".")) filename = $"{AppContext.BaseDirectory}/{filename}";
                browserScripts = JsonConvert.DeserializeObject<BrowserScripts>(File.ReadAllText(filename));
            }
            else
            {
                browserScripts = await s3.ReadFromJson<BrowserScripts>(scriptFileObj.Key, scriptFileObj.BucketName);
            }


            // run each scripts

            Scripts = browserScripts;
            if (browserScripts.Data == null)
            {
                browserScripts.Data = new Dictionary<string, string>();
            }

            // load the JObject payload data from the messsage
            Jsons = new Dictionary<string, JObject>();
            if (job.Payload != null)
            {
                foreach (var property in job.Payload.Properties())
                {
                    if (property.HasValues)
                    {
                        // we will store JObject in Jsons and key-value pairs in Dictionary
                        switch (property.Value.Type)
                        {
                            case JTokenType.Object:
                                Jsons.Add(property.Name, property.Value<JObject>());
                                break;
                            case JTokenType.String:
                                browserScripts.Data.Add(property.Name, property.Value.Value<string>());
                                break;
                            case JTokenType.Integer:
                                browserScripts.Data.Add(property.Name, property.Value.Value<string>());
                                break;
                            case JTokenType.Float:
                                browserScripts.Data.Add(property.Name, property.Value.Value<string>());
                                break;
                            case JTokenType.Boolean:
                                browserScripts.Data.Add(property.Name, property.Value.Value<string>());
                                break;
                            case JTokenType.Date:
                                browserScripts.Data.Add(property.Name, property.Value.Value<string>());
                                break;
                            default:
                                Console.WriteLine($"Payload Property {property.Name} Ignored. Property type {property.Value.Type} is not supported!");
                                break;
                        }
                    }
                }
            }

            foreach (var key in Scripts.Run)
            {
                if (browserScripts.Data.ContainsKey("success"))
                {
                    browserScripts.Data["success"] = "false";
                }
                else
                {
                    browserScripts.Data.Add("success", "false");
                }
                RunScript(key, browserScripts.Data);
                switch (browserScripts.Data["success"])
                {
                    case "true":
                        Console.WriteLine($"Job Succeeded: {job.Script} {job.Job} -> {key}");
                        await scheduler.CompleteScriptJob(new ScriptJobMessage()
                        {
                            Job = job,
                            ReceiptHandle = null
                        }, true);
                        break;
                    case "false":
                        Console.WriteLine($"Job Failed: {job.Script} {job.Job} -> {key}");
                        break;
                }
            }

            if (options.MultipleJobs)
            {
                Console.WriteLine($"MultipleJobs Mode. Try another job now:");
            }
        }
    }
}
