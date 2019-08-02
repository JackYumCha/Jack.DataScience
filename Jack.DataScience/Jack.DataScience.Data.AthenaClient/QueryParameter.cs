using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace Jack.DataScience.Data.AthenaClient
{
    public class QueryParameter: INotifyPropertyChanged
    {
        private string _Key;

        public string Key
        {
            get => _Key;
            set
            {
                if (_Key != value)
                {
                    _Key = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Key)));
                }
            }
        }


        private QueryParameterTypeEnum _Type;
        public QueryParameterTypeEnum Type
        {
            get => _Type;
            set
            {
                if (_Type != value)
                {
                    _Type = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Type)));
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

        private string _Value;

        public string Value
        {
            get => _Value;
            set
            {
                if (_Value != value)
                {
                    _Value = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Value)));
                }
            }
        }


        private string _RegexPattern;

        public string RegexPattern
        {
            get => _RegexPattern;
            set
            {
                if (_RegexPattern != value)
                {
                    _RegexPattern = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(RegexPattern)));
                }
            }
        }

        private string _AthenaQuery;

        public string AthenaQuery
        {
            get => _AthenaQuery;
            set
            {
                if (_AthenaQuery != value)
                {
                    _AthenaQuery = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(AthenaQuery)));
                }
            }
        }

        private List<string> _Values;

        public List<string> Values
        {
            get => _Values;
            set
            {
                if (_Values != value)
                {
                    _Values = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Values)));
                }
            }
        }

        public event PropertyChangedEventHandler PropertyChanged;
    }


    public static class PropertyChangedEventHandlerExtensions
    {
        public static void Set<T>(this PropertyChangedEventHandler handler, T value, string name)
        {
            
        }
    }
}
