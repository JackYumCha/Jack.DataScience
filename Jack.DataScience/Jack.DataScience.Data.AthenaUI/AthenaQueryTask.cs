using Jack.DataScience.Data.AthenaClient;
using Jack.DataScience.Data.AWSAthena;
using Jack.DataScience.Data.CSV;
using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace Jack.DataScience.Data.AthenaUI
{
    public class AthenaQueryTask: INotifyPropertyChanged
    {
        private string _JobId;
        public string JobId
        {
            get => _JobId;
            set
            {
                if (_JobId != value)
                {
                    _JobId = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(JobId)));
                }
            }
        }

        private string _Query;
        public string Query
        {
            get => _Query;
            set
            {
                if (_Query != value)
                {
                    _Query = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Query)));
                }
            }
        }

        private string _StartTime;
        public string StartTime
        {
            get => _StartTime;
            set
            {
                if (_StartTime != value)
                {
                    _StartTime = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(StartTime)));
                }
            }
        }

        private string _EndTime;
        public string EndTime
        {
            get => _EndTime;
            set
            {
                if (_EndTime != value)
                {
                    _EndTime = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(EndTime)));
                }
            }
        }

        private string _Status;
        public string Status
        {
            get => _Status;
            set
            {
                if (_Status != value)
                {
                    _Status = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Status)));
                }
            }
        }

        private string _Filename;
        public string Filename
        {
            get => _Filename;
            set
            {
                if (_Filename != value)
                {
                    _Filename = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Filename)));
                }
            }
        }

        private string _Note;
        public string Note
        {
            get => _Note;
            set
            {
                if (_Note != value)
                {
                    _Note = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Note)));
                }
            }
        }

        private string _FileSize;

        public string FileSize
        {
            get => _FileSize;
            set
            {
                if (_FileSize != value)
                {
                    _FileSize = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(FileSize)));
                }
            }
        }

        public event PropertyChangedEventHandler PropertyChanged;

        public async Task ExecuteJob(AWSAthenaAPI athena, FormatedQuery query, string filePath, string jobID)
        {

            JobId = jobID;
            StartTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            Status = "Running";
            Query = query.BuildQuerySQL();

            AthenaQueryFlatResult data = null;
            try
            {
                data = await athena.GetQueryData(query);
            }
            catch(Exception ex)
            {
                Status = "Failed";
                Note = ex.Message;
                return;   
            }

            Note = data.Note;

            while(filePath == null)
            {
                SaveFileDialog saveFileDialog = new SaveFileDialog()
                {
                    Filter = "*.csv|CSV File",
                };
                var result = saveFileDialog.ShowDialog();
                if (result.HasValue && result.Value)
                {
                    filePath = saveFileDialog.FileName;
                }
                
                if(filePath == null)
                {
                    var msgResult = MessageBox.Show("Do you want to abandon the results? Click No to Choose a valid file path.", "Warning - Data Not Saved", MessageBoxButton.YesNo);
                    switch (msgResult)
                    {
                        case MessageBoxResult.Yes:
                            return;
                        case MessageBoxResult.No:
                            break;
                    }
                }
            }

            FileInfo file = new FileInfo(filePath);
            var dir = file.Directory;
            while (dir != null)
            {
                if (!Directory.Exists(dir.FullName)) Directory.CreateDirectory(dir.FullName);
                dir = dir.Parent;
            }

            CsvFile.Dump(filePath, data.Data, data.Columns);

            var fileInfo = new FileInfo(filePath);
            double length = fileInfo.Length;
            int level = 0;
            while(length >= 1024d)
            {
                length = length / 1024d;
                level += 1;
            }
            FileSize = level > 0 ? $"{length.ToString("0.00")}{units[level]}" : $"{length}{units[level]}";
            EndTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            Status = "Completed";
            Filename = filePath;
        }

        static string[] units = new string[] { "B", "KB", "MB", "GB", "TB" };
    }
}
