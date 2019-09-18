using Jack.DataScience.ProcessExtensions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

namespace Jack.DataScience.ProcessControl
{
    public class ProcessControlAPI: IDisposable
    {
        private readonly ProcessControlOptions processControlOptions;
        private List<IDisposable> Subscriptions = new List<IDisposable>();
        private List<ProcessExecutor> processExecutors = new List<ProcessExecutor>();
        private Dictionary<string, StreamTimeoutChecker> streamTimeoutCheckers = new Dictionary<string, StreamTimeoutChecker>();
        public ProcessControlAPI(ProcessControlOptions processControlOptions)
        {
            this.processControlOptions = processControlOptions;
        }

        public Subject<string> StandardOutput { get; private set; } = new Subject<string>();
        public Subject<string> StandardError { get; private set; } = new Subject<string>();

        private int JobCompleted;

        public Task Start()
        {
            Task task = new Task(() =>
            {
                BeginWatch();
                while (Thread.VolatileRead(ref JobCompleted) == 0)
                {
                    Thread.Sleep(processControlOptions.Interval);
                    CheckTimeout();
                }
            });
            task.Start();
            return task;
        }

        private void CheckTimeout()
        {
            foreach(var checker in streamTimeoutCheckers.Values.ToArray())
            {
                StandardOutput.OnNext($"[{nameof(CheckTimeout)}]{checker.StreamName}: {checker.Stopwatch.Elapsed.TotalSeconds.ToString("0.00")}");
                if (checker.Stopwatch.Elapsed.TotalSeconds > checker.Timeout)
                {
                    OnExit(checker.StreamName, 408);
                    break;
                }
            }
        }

        public void OnExit(string streamName, int exitCode)
        {
            StandardError.OnNext($"[{nameof(OnExit)}] Process '{streamName}' exited with Code {exitCode};");
            StandardOutput.OnNext($"[{nameof(OnExit)}] Kill Processes");
            streamTimeoutCheckers.Clear();
            Subscriptions.ForEach(x => x.Dispose());
            Subscriptions.Clear();
            processExecutors.ForEach(p => p.TryKill());
            StandardOutput.OnNext($"[{nameof(OnExit)}] {processControlOptions.Retry} Retries Remaining.");
            processControlOptions.Retry--;
            if(processControlOptions.Retry >= 0)
            {
                StandardOutput.OnNext($"[{nameof(OnExit)}] Retry now.");
                BeginWatch();
            }
            else
            {
                KillProcesses();
                JobCompleted = 1;
            }
        }

        public void BeginWatch()
        {
            KillProcesses();
            StandardOutput.OnNext($"[{nameof(BeginWatch)}] Starting Processes.");
            var executors = processControlOptions.ProcessesToStart.Select(monitorOptions =>
            {
                ProcessExecutor processExecutor = new ProcessExecutor(monitorOptions.ProcessPath.PopulateKeys());
                processExecutor.AddArguments(monitorOptions.Arguments.Select(argument => argument.PopulateKeys()));
                return new
                {
                    processExecutor,
                    monitorOptions
                };
            }).ToArray();
            foreach(var executor in executors)
            {
                Subscriptions.Add(executor.processExecutor.StandardOutput
                    .Subscribe((string value) =>
                    {
                        if(executor.monitorOptions.HeartBeatKeys.Any(key => !string.IsNullOrEmpty(value) && value.Contains(key))) {
                            // heart beat is consumed internally
                            streamTimeoutCheckers[executor.monitorOptions.StreamName].Stopwatch.Restart();
                        }
                        else
                        {
                            streamTimeoutCheckers[executor.monitorOptions.StreamName].Stopwatch.Restart();
                            StandardOutput.OnNext($"{executor.monitorOptions.StreamName}: {value}");
                        }
                    }));
                Subscriptions.Add(executor.processExecutor.StandardError
                    .Subscribe((string value) =>
                    {
                        streamTimeoutCheckers[executor.monitorOptions.StreamName].Stopwatch.Restart();
                        StandardError.OnNext($"{executor.monitorOptions.StreamName}: {value}");
                    }));
                Subscriptions.Add(executor.processExecutor.OnExit.Subscribe((int code) => OnExit(executor.monitorOptions.StreamName, code)));
                processExecutors.Add(executor.processExecutor);
                StreamTimeoutChecker checker = new StreamTimeoutChecker()
                {
                    StreamName = executor.monitorOptions.StreamName,
                    Timeout = executor.monitorOptions.Timeout,
                    Stopwatch = new Stopwatch()
                };
                checker.Stopwatch.Start();
                streamTimeoutCheckers.Add(checker.StreamName, checker);
                executor.processExecutor.ExecuteAsync();
            }
            StandardOutput.OnNext($"[{nameof(BeginWatch)}] {executors.Length} Processes Started.");
        }

        public void KillProcesses()
        {
            if (processControlOptions.ProcessesToKillOnError == null) return;
            StandardOutput.OnNext($"[{nameof(KillProcesses)}] Search for Conflicting Processes.");
            Dictionary<int, Process> processDict = new Dictionary<int, Process>();
            foreach(var processName in processControlOptions.ProcessesToKillOnError)
            {
                var found =  Process.GetProcessesByName(processName);
                if (found.Any())
                {
                    foreach(var process in found)
                    {
                        if (!processDict.ContainsKey(process.Id))
                            processDict.Add(process.Id, process);
                    }
                }
            }
            StandardOutput.OnNext($"[{nameof(KillProcesses)}] {processDict.Count} Processes Found: {string.Join(", ", processDict.Values.Select(p => p.ProcessName))}");
            foreach (var process in processDict.Values)
            {
                try
                {
                    if(!process.HasExited)
                        process.Kill();
                }
                catch (Exception ex) { }
            }
        }

        public void Dispose()
        {
            streamTimeoutCheckers.Clear();
            Subscriptions.ForEach(x => x.Dispose());
            KillProcesses();
        }
    }
}
