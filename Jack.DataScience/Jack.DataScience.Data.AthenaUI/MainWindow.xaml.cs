using Autofac;
using CsvHelper;
using Jack.DataScience.Data.AthenaClient;
using Jack.DataScience.Data.AWSAthena;
using Jack.DataScience.Data.CSV;
using Jack.DataScience.Storage.AWSS3;
using Jack.DataScience.WPF.Controls;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
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
    /// MainWindow.xaml 的交互逻辑
    /// </summary>
    public partial class MainWindow : Window
    {

        IComponentContext componentContext;
        ObservableCollection<S3TreeItem> S3ItemsSource = new ObservableCollection<S3TreeItem>();
        ObservableCollection<FormatedQuery> FormatedQuerySource = new ObservableCollection<FormatedQuery>();
        ObservableCollection<AthenaQueryTask> QueryTasks = new ObservableCollection<AthenaQueryTask>();
        AWSS3API s3;
        AWSAthenaAPI athena;
        AthenaClientOptions options;

        public MainWindow()
        {
            componentContext = (App.Current as App).Services;

            var encryptedOptions = componentContext.Resolve<EncryptedOptions>();
 
            DecryptedOptions decryptedOptions = LoadOptions(encryptedOptions);
            if(decryptedOptions == null)
            {
                Application.Current.Shutdown();
                return;
            }
            s3 = new AWSS3API(decryptedOptions.AWSS3Options);
            athena = new AWSAthenaAPI(decryptedOptions.AWSAthenaOptions);
            options = decryptedOptions.AthenaClientOptions;
            InitializeComponent();
            s3Tree.ItemsSource = S3ItemsSource;
            tabQueries.ItemsSource = FormatedQuerySource;
            dgJobList.ItemsSource = QueryTasks;
            LoadS3();
        }


        private DecryptedOptions LoadOptions(EncryptedOptions encryptedOptions)
        {
            

            DecryptedOptions decryptedOptions = null;
            bool needLogin = true;
            while (needLogin)
            {
                LoginWindow loginWindow = new LoginWindow();
                var result = loginWindow.ShowDialog();

                if (!result.HasValue || !result.Value)
                {
                    Application.Current.Shutdown();
                    return null;
                }

                var username = loginWindow.Username;
                var password = loginWindow.Password;

                var base64IV = "NhbsNbMmiZo3Ql4wMorPMg==";
                var base64Key = "zFJnXBUqQVW+0UTexm79qT4muUVHaypmQjoFN1mGJog=";
                var IV = Convert.FromBase64String(base64IV);
                var Key = Convert.FromBase64String(base64Key);

                var aes = Aes.Create();
                aes.KeySize = 256;
                var secret = $"{username}+{password}";
                var secretBytes = Encoding.UTF8.GetBytes(secret);
                Array.Copy(secretBytes, Key, secretBytes.Length);
                aes.IV = IV;
                aes.Key = Key;

                var encrypted = Convert.FromBase64String(encryptedOptions.Value);

                byte[] data = null;

                try
                {
                    data = aes.CreateDecryptor().TransformFinalBlock(encrypted, 0, encrypted.Length);
                    needLogin = false;
                }
                catch (CryptographicException ex)
                {
                    var msgResult = MessageBox.Show("Wrong Username or Password! Do you want to try again?",
                        "Login Error",
                        MessageBoxButton.YesNo);

                    if (msgResult == MessageBoxResult.No)
                    {
                        return null;
                    }
                    else if(msgResult == MessageBoxResult.Yes)
                    {
                        continue;
                    }
                }

                var json = Encoding.UTF8.GetString(data);

                decryptedOptions = JsonConvert.DeserializeObject<DecryptedOptions>(json);
            }



            return decryptedOptions;
        }

        public async Task LoadS3()
        {
            var paths = await s3.ListPaths("", "/");
            S3ItemsSource.Clear();

            var root = new S3TreeRoot(s3);
            S3ItemsSource.Add(root);
            await root.Load();
        }


        private async void FileItemClick(object sender, MouseButtonEventArgs e)
        {
            if (e.ClickCount == 2)
            {
                var panel = sender as StackPanel;
                var file = panel.DataContext as S3TreeFile;
                if (file == null) return;
                await file.Load();
                if (!FormatedQuerySource.Contains(file.Query))
                {
                    FormatedQuerySource.Add(file.Query);
                }
                tabQueries.SelectedItem = file.Query;
            }
        }

        private async void EditQuery(object sender, RoutedEventArgs e)
        {
            var file = s3Tree.SelectedItem as S3TreeFile;
            if (file == null) return;
            await file.Load();
            if (!FormatedQuerySource.Contains(file.Query))
            {
                FormatedQuerySource.Add(file.Query);
            }
            tabQueries.SelectedItem = file.Query;
        }


        private void CreateQuery(object sender, RoutedEventArgs e)
        {
            var query = new FormatedQuery()
            {
                Name = "New Query",
                Description = "New Query",
                Query = @"Select * From db.table
Where condition = <#parameter#>;",
                Parameters = new ObservableCollection<QueryParameter>()
            };
            FormatedQuerySource.Add(query);
            tabQueries.SelectedItem = query;
        }

        private async void SaveQuery(object sender, RoutedEventArgs e)
        {
            FormatedQuery query = tabQueries.SelectedItem as FormatedQuery;
            if (query == null) return;
            if (query.S3Path != null)
            {
                await s3.UploadAsJson(query.S3Path, query);
            }
            else
            {
                var address = AddressSelectionBox.GetAddressInput();
                if (address == null) return;
                try
                {
                    await s3.UploadAsJson(address, query);
                    await LoadS3();
                    query.Changed = false;
                }
                catch (Exception ex)
                {
                    MessageBox.Show(ex.Message);
                    return;
                }
            }
        }

        private async void SaveQueryAs(object sender, RoutedEventArgs e)
        {
            FormatedQuery query = tabQueries.SelectedItem as FormatedQuery;
            if (query == null) return;
            var address = AddressSelectionBox.GetAddressInput(query.S3Path);
            if (address == null) return;
            try
            {
                await s3.UploadAsJson(address, query);
                await LoadS3();
                query.Changed = false;
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
                return;
            }
        }

        private async void DeleteQuery(object sender, RoutedEventArgs e)
        {
            var file = s3Tree.SelectedItem as S3TreeFile;
            if (file == null) return;
            var query = file.Query;
            if (query == null)
            {
                // load the query 
                try
                {
                    await file.Load();
                    query = file.Query;
                }
                catch (Exception ex)
                {
                    query = new FormatedQuery() { Name = "Failed to Load!" };
                }
            }

            switch (MessageBox.Show($"Do you want to delete '{file.S3Path}' ({query.Name})?", "Deletion Confirm", MessageBoxButton.OKCancel))
            {
                case MessageBoxResult.Cancel:
                    return;
            }

            if (FormatedQuerySource.Contains(query))
            {
                // remove from view
                FormatedQuerySource.Remove(query);
            }

            await s3.Delete(file.PathFromRoot());
            // delete from the treeview
            file.Parent.Items.Remove(file);
        }

        private async void CloseQueryView(object sender, RoutedEventArgs e)
        {
            var el = sender as FrameworkElement;
            var query = el.DataContext as FormatedQuery;
            if (query.Changed)
            {
                switch (MessageBox.Show("Do you want to save the query?", "Warning - Unsaved", MessageBoxButton.YesNoCancel))
                {
                    case MessageBoxResult.Yes:
                        // save
                        if (query.S3Path != null && await s3.FileExists(query.S3Path))
                        {
                            await s3.UploadAsJson(query.S3Path, query);
                            var node = FindTreeItem(query);
                            if (node != null) await node.Load();
                            query.Changed = false;
                        }
                        else
                        {
                            var address = AddressSelectionBox.GetAddressInput();
                            if (address == null) return;
                            try
                            {
                                await s3.UploadAsJson(address, query);
                            }
                            catch (Exception ex)
                            {
                                MessageBox.Show(ex.Message);
                                return;
                            }
                        }
                        break;
                    case MessageBoxResult.No:
                        // load the file again from s3
                        if (query.S3Path != null && await s3.FileExists(query.S3Path))
                        {
                            var node = FindTreeItem(query);
                            if (node != null) await node.Load();
                        }
                        break;
                    case MessageBoxResult.Cancel:
                        return;
                }
            }
            FormatedQuerySource.Remove(query);
        }

        private S3TreeFile FindTreeItem(FormatedQuery query)
        {
            if (s3Tree.Items.Count == 0) return null;
            var root = s3Tree.Items.Cast<S3TreeRoot>().First();
            string path = query.S3Path;
            S3TreeItem node;
            do
            {
                node = root.Items.Where(p => path.StartsWith(p.S3Path)).FirstOrDefault();
                if (node != null) path = path.Substring(node.S3Path.Length);

            } while (node is S3TreePath);
            return node as S3TreeFile;
        }


        private async void RefreshQueries(object sender, RoutedEventArgs e)
        {
            if (s3Tree.SelectedItem == null)
            {
                await LoadS3();
            }
            else
            {
                S3TreeItem item = s3Tree.SelectedItem as S3TreeItem;
                switch (item.FileType)
                {
                    case "root":
                    case "path":
                        await item.Load();
                        break;
                }
            }
        }

        private void QueryChanged(object sender, TextChangedEventArgs e)
        {
            var frameworkElement = sender as FrameworkElement;
            if (frameworkElement != null)
            {
                var query = frameworkElement.DataContext as FormatedQuery;
                if (query == null) return;
                query.Changed = true;
                try
                {
                    query.SQL = query.BuildQuerySQL();
                }
                catch (Exception ex) { }
            }
        }

        private void AddQueryParameter(object sender, RoutedEventArgs e)
        {
            FormatedQuery query = (sender as FrameworkElement)?.DataContext as FormatedQuery;
            if (query == null) return;
            query.Parameters.Add(new QueryParameter()
            {
                Key = "New Parameter",
                Value = "",
                RegexPattern = "",
                AthenaQuery = "",
                Type = QueryParameterTypeEnum.String,
                Description = "New Parameter"
            });
        }

        private void MoveParameterUpwards(object sender, RoutedEventArgs e)
        {
            FormatedQuery query = (sender as FrameworkElement)?.DataContext as FormatedQuery;
            if (query == null) return;
            var dgParameters = sender
                .As<FrameworkElement>()?
                .Parents<Grid>()?
                .FirstOrDefault(gd => gd.Name == "gridQuery")?
                .Children<DataGrid>()?
                .FirstOrDefault(dg => dg.Name == "dgParameters");
            if (dgParameters == null) return;
            var selectedParameter = dgParameters.SelectedItem as QueryParameter;
            if (query.Parameters.Contains(selectedParameter))
            {
                int index = query.Parameters.IndexOf(selectedParameter);
                if (index > 0)
                {
                    query.Parameters.RemoveAt(index);
                    index--;
                    query.Parameters.Insert(index, selectedParameter);
                    dgParameters.SelectedItem = selectedParameter;
                }
            }
        }

        private void MoveParameterDownwards(object sender, RoutedEventArgs e)
        {
            FormatedQuery query = (sender as FrameworkElement)?.DataContext as FormatedQuery;
            if (query == null) return;
            var dgParameters = sender
    .As<FrameworkElement>()?
    .Parents<Grid>()?
    .FirstOrDefault(gd => gd.Name == "gridQuery")?
    .Children<DataGrid>()?
    .FirstOrDefault(dg => dg.Name == "dgParameters");
            if (dgParameters == null) return;
            var selectedParameter = dgParameters.SelectedItem as QueryParameter;
            if (query.Parameters.Contains(selectedParameter))
            {
                int index = query.Parameters.IndexOf(selectedParameter);
                if (index < query.Parameters.Count - 1)
                {
                    query.Parameters.RemoveAt(index);
                    index++;
                    query.Parameters.Insert(index, selectedParameter);
                    dgParameters.SelectedItem = selectedParameter;
                }
            }
        }

        private void DeleteQueryParameter(object sender, RoutedEventArgs e)
        {
            var frameworkElement = sender as FrameworkElement;
            if (frameworkElement == null) return;
            FormatedQuery query = frameworkElement.DataContext as FormatedQuery;
            if (query == null) return;
            Grid host = frameworkElement.Parents<Grid>().FirstOrDefault(gd => gd.Name == "gridQuery") as Grid;
            if (host == null) return;
            DataGrid dgParameters = host.Children<DataGrid>().FirstOrDefault(dg => dg.Name == "dgParameters");
            var selectedParameter = dgParameters.SelectedItem as QueryParameter;
            if (selectedParameter is QueryParameter)
            {
                query.Parameters.Remove(selectedParameter);
            }
        }

        private async void ExecuteAndDownload(object sender, RoutedEventArgs e)
        {
            var query = sender.As<FrameworkElement>()?.DataContext?.As<FormatedQuery>();
            if (query == null) return;
            if (string.IsNullOrWhiteSpace(query.DefaultExportPath))
            {
                MessageBox.Show("Default Export Path is Emapty. Please set a proper default path.", "Error", MessageBoxButton.OK);
                return;
            }
            //var data = await athena.GetQueryData(query);
            string filePath = $"{options.DataRootPath}/{query.DefaultExportPath}.{DateTime.Now.ToString("yyyyMMdd.HHmmss")}.csv";

            if (options.DataRootPath.StartsWith("."))
            {
                filePath = AppContext.BaseDirectory + "/" + filePath;
            }

            //CsvFile.Dump(filePath, data.Data, data.Columns);

            AthenaQueryTask queryTask = new AthenaQueryTask();
            QueryTasks.Insert(0, queryTask);
            await queryTask.ExecuteJob(athena, query, filePath, QueryTasks.Count.ToString());

                
        }

        private async void ExecuteAndDownloadAs(object sender, RoutedEventArgs e)
        {
            var query = sender.As<FrameworkElement>()?.DataContext?.As<FormatedQuery>();
            if (query == null) return;
            if (string.IsNullOrWhiteSpace(query.DefaultExportPath))
            {
                MessageBox.Show("Default Export Path is Emapty. Please set a proper default path.", "Error", MessageBoxButton.OK);
                return;
            }
            //var data = await athena.GetQueryData(query);
            string filePath = $"{options.DataRootPath}/{query.DefaultExportPath}.{DateTime.Now.ToString("yyyyMMdd.HHmmss")}.csv";

            //if (options.DataRootPath.StartsWith("."))
            //{
            //    filePath = AppContext.BaseDirectory + "/" + filePath;
            //}

            //CsvFile.Dump(filePath, data.Data, data.Columns);

            AthenaQueryTask queryTask = new AthenaQueryTask();
            QueryTasks.Insert(0, queryTask);
            await queryTask.ExecuteJob(athena, query, filePath, QueryTasks.Count.ToString());
        }


        private void FormatedQueryTemplateLoaded(object sender, RoutedEventArgs e)
        {
            var grid = sender as Grid;
            var query = grid.DataContext as FormatedQuery;
            if (query == null) return;
            try
            {
                query.SQL = query.BuildQuerySQL();
            }
            catch (Exception ex) { }

            var dgParameters = grid.Children<DataGrid>().FirstOrDefault(dg => dg.Name == "dgParameters");
            foreach (var column in dgParameters.Columns)
            {
                if (column.Header.ToString() == "Value") continue;
                column.IsReadOnly = true;
            }
        }

        private void ChangeToNotEditable(object sender, RoutedEventArgs e)
        {
            var frameworkElement = sender as FrameworkElement;
            if (frameworkElement == null) return;
            Grid host = frameworkElement.Parents<Grid>().FirstOrDefault(gd => gd.Name == "gridQuery") as Grid;
            if (host == null) return;
            var dgParameters = host.Children<DataGrid>().FirstOrDefault(dg => dg.Name == "dgParameters");
            foreach (var column in dgParameters.Columns)
            {
                if (column.Header.ToString() == "Value") continue;
                column.IsReadOnly = true;
            }
        }

        private void ChangeToEditable(object sender, RoutedEventArgs e)
        {
            var frameworkElement = sender as FrameworkElement;
            if (frameworkElement == null) return;
            Grid host = frameworkElement.Parents<Grid>().FirstOrDefault(gd => gd.Name == "gridQuery") as Grid;
            if (host == null) return;
            var dgParameters = host.Children<DataGrid>().FirstOrDefault(dg => dg.Name == "dgParameters");
            foreach (var column in dgParameters.Columns)
            {
                if (column.Header.ToString() == "Value") continue;
                column.IsReadOnly = false;
            }
        }

        private void OnlyDigits(object sender, KeyEventArgs e)
        {
            if ((Key.NumPad0 <= e.Key && e.Key <= Key.NumPad9) || 
                (Key.D0 <= e.Key && e.Key <= Key.D9) ||
                e.Key == Key.Left || e.Key == Key.Right || e.Key == Key.Up || e.Key == Key.Down ||
                e.Key == Key.Delete ||
                e.Key == Key.Back) return;
            e.Handled = true;
        }

        private void OnlyDigitsAndDot(object sender, KeyEventArgs e)
        {
            if ((Key.NumPad0 <= e.Key && e.Key <= Key.NumPad9) ||
    (Key.D0 <= e.Key && e.Key <= Key.D9) ||
    e.Key == Key.Left || e.Key == Key.Right || e.Key == Key.Up || e.Key == Key.Down ||
    e.Key == Key.Decimal || e.Key == Key.OemPeriod ||
    e.Key == Key.Delete ||
    e.Key == Key.Back) return;
            e.Handled = true;
        }

        private async void GenerateValues(object sender, RoutedEventArgs e)
        {
            var queryParameter = ((sender as FrameworkElement)?.DataContext as QueryParameter);
            if (queryParameter == null) return;
            try
            {
                var results = await athena.GetQueryResults<AthenaValueItem>(queryParameter.AthenaQuery);
                queryParameter.Values = results.Select(i => i.Item).ToList();
            }
            catch(Exception ex) { }
        }

        private void ParameterValueChanged(object sender, EventArgs e)
        {
            var frameworkElement = sender as FrameworkElement;
            var grid = frameworkElement.Parents<Grid>().FirstOrDefault(gd => gd.Name == "gridQuery");
            if (grid != null)
            {
                var query = grid.DataContext as FormatedQuery;
                if (query == null) return;
                query.Changed = true;
                try
                {
                    query.SQL = query.BuildQuerySQL();
                }
                catch (Exception ex) { }
            }
        }

        private void OpenDownloadedFile(object sender, RoutedEventArgs e)
        {
            var frameworkElement = sender as FrameworkElement;
            var queryTask = frameworkElement?.DataContext as AthenaQueryTask;
            if (queryTask == null || queryTask.Status != "Completed" ) return;
            Process.Start(queryTask.Filename);
        }
    }
}
