using Jack.DataScience.ProcessExtensions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive;
using System.Reactive.Subjects;

namespace Jack.DataScience.ProcessControl
{
    public class ProcessControlAPI
    {
        private readonly ProcessControlOptions processControlOptions;
        private List<IDisposable> Subscriptions = new List<IDisposable>();
        private List<ProcessExecutor> processExecutors = new List<ProcessExecutor>();
        public ProcessControlAPI(ProcessControlOptions processControlOptions)
        {
            this.processControlOptions = processControlOptions;
        }

        public Subject<string> StandardOutput { get; private set; } = new Subject<string>();
        public Subject<string> StandardError { get; private set; } = new Subject<string>();


        public void OnExit(string streamName, int exitCode)
        {
            StandardError.OnNext($"[{nameof(OnExit)}] Process '{streamName}' exited with Code {exitCode};");
            StandardOutput.OnNext($"[{nameof(OnExit)}] Kill Processes");
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
        }

        public void BeginWatch()
        {
            
            KillProcesses();
            StandardOutput.OnNext($"[{nameof(BeginWatch)}] Starting Processes.");
            var executors = processControlOptions.ProcessesToStart.Select(monitorOptions =>
            {
                ProcessExecutor processExecutor = new ProcessExecutor(monitorOptions.ProcessPath);
                processExecutor.AddArguments(monitorOptions.Arguments);
                return new
                {
                    processExecutor,
                    monitorOptions
                };
            }).ToArray();
            foreach(var executor in executors)
            {
                Subscriptions.Add(executor.processExecutor.StandardOutput
                    .Subscribe((string value) => StandardOutput.OnNext($"{executor.monitorOptions.StreamName}: {value}")));
                Subscriptions.Add(executor.processExecutor.StandardError
                    .Subscribe((string value) => StandardError.OnNext($"{executor.monitorOptions.StreamName}: {value}")));
                Subscriptions.Add(executor.processExecutor.OnExit.Subscribe((int code) => OnExit(executor.monitorOptions.StreamName, code)));
                processExecutors.Add(executor.processExecutor);
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
            StandardOutput.OnNext($"[{nameof(KillProcesses)}] {processDict.Count} Processes Found.");
            foreach (var process in processDict.Values)
            {
                try
                {
                    process.Kill();
                }
                catch (Exception ex) { }
            }
        }
    }
}
