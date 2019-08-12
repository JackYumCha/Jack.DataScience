using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace Jack.DataScience.Data.AthenaUI
{
    /// <summary>
    /// LoginWindow.xaml 的交互逻辑
    /// </summary>
    public partial class LoginWindow : Window
    {
        public LoginWindow()
        {
            InitializeComponent();
        }

        public string Username { get; set; }
        public string Password { get; set; }
        private void SetUsername(object sender, TextChangedEventArgs e)
        {
            Username = tbUsername.Text;
        }
        private void SetPassword(object sender, RoutedEventArgs e)
        {
            Password = pbPassword.Password;
        }

        private void Cancel(object sender, RoutedEventArgs e)
        {
            DialogResult = false;
        }

        private void Login(object sender, RoutedEventArgs e)
        {
            DialogResult = true;
        }


    }
}
