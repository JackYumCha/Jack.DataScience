using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace Jack.DataScience.Data.AthenaClient
{
    public class QueryParameter: INotifyPropertyChanged
    { 
        public QueryParameter()
        {
            _BindingValue = new ValueViewModel<string>(this);
        }

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
                    switch (_Type)
                    {
                        case QueryParameterTypeEnum.String:
                            _BindingValue = new ValueViewModel<string>(this);
                            break;
                        case QueryParameterTypeEnum.Double:
                            double doubleValue;
                            if (!double.TryParse(_Value, out doubleValue))
                            {
                                _Value = 0d.ToString();
                                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Value)));
                            }
                            _BindingValue = new ValueViewModel<double>(this);
                            break;
                        case QueryParameterTypeEnum.Integer:
                            long longValue;
                            if (!long.TryParse(_Value, out longValue))
                            {
                                _Value = 0L.ToString();
                                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Value)));
                            }
                            _BindingValue = new ValueViewModel<long>(this);
                            break;
                        case QueryParameterTypeEnum.Boolean:
                            bool boolValue;
                            if (!bool.TryParse(_Value, out boolValue))
                            {
                                _Value = false.ToString().ToLower();
                                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Value)));
                            }
                            _BindingValue = new ValueViewModel<bool>(this);
                            break;
                        case QueryParameterTypeEnum.SpecialFormat:
                            _BindingValue = new ValueViewModel<string>(this);
                            break;
                        case QueryParameterTypeEnum.QueryResult:
                            _BindingValue = new ValueViewModel<string>(this);
                            break;
                    }
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(BindingValue)));
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
                    _BindingValue?.OnValueChange(nameof(ValueViewModel<object>.Value));
                }
            }
        }

        private ValueViewModel _BindingValue;
        public ValueViewModel BindingValue
        {
            get => _BindingValue;
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
                    _BindingValue?.OnValueChange(nameof(ValueViewModel<object>.Values));
                }
            }
        }

        public event PropertyChangedEventHandler PropertyChanged;

        public void OnValueChange(string propertyName)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }


    public static class PropertyChangedEventHandlerExtensions
    {
        public static void Set<T>(this PropertyChangedEventHandler handler, T value, string name)
        {
            
        }
    }
}
