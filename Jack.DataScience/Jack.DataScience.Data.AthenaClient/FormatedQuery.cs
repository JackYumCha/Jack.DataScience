using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;

namespace Jack.DataScience.Data.AthenaClient
{
    public class FormatedQuery: INotifyPropertyChanged  
    {

        private string _S3Path;

        public string S3Path
        {
            get => _S3Path;
            set
            {
                if (_S3Path != value)
                {
                    _S3Path = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(S3Path)));
                }
            }
        }

        private ObservableCollection<QueryParameter> _Parameters;

        public ObservableCollection<QueryParameter> Parameters
        {
            get => _Parameters;
            set
            {
                if (_Parameters != value)
                {
                    _Parameters = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Parameters)));
                }
            }
        }

        private string _Name;

        public string Name
        {
            get => _Name;
            set
            {
                if (_Name != value)
                {
                    _Name = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Name)));
                }
            }
        }

        private string _Description;

        public string Description
        {
            get => _Description;
            set
            {
                if (_Description != value)
                {
                    _Description = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Description)));
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

        private string _SQL;

        [JsonIgnore]
        public string SQL
        {
            get => _SQL;
            set
            {
                if (_SQL != value)
                {
                    _SQL = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(SQL)));
                }
            }
        }


        private bool _Changed;

        [JsonIgnore]
        public bool Changed
        {
            get => _Changed;
            set
            {
                if (_Changed != value)
                {
                    _Changed = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Changed)));
                }
            }
        }

        private bool _Editable;
        [JsonIgnore]
        public bool Editable
        {
            get => _Editable;
            set
            {
                if (_Editable != value)
                {
                    _Editable = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Editable)));
                }
            }
        }


        private string _DefaultExportPath;

        public string DefaultExportPath
        {
            get => _DefaultExportPath;
            set
            {
                if (_DefaultExportPath != value)
                {
                    _DefaultExportPath = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(DefaultExportPath)));
                }
            }
        }

        public event PropertyChangedEventHandler PropertyChanged;
    }
}
