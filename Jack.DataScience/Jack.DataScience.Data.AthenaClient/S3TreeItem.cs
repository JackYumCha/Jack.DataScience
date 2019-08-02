using Jack.DataScience.Storage.AWSS3;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Text;
using System.Threading.Tasks;

namespace Jack.DataScience.Data.AthenaClient
{
    public class S3TreeItem : INotifyPropertyChanged
    {
        protected readonly AWSS3API awsS3API;
        public S3TreeItem(AWSS3API awsS3API)
        {
            this.awsS3API = awsS3API;
        }

        private string _Title;

        public string Title
        {
            get => _Title;
            set
            {
                if (_Title != value)
                {
                    _Title = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Title)));
                }
            }
        }

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

        public string FileType
        {
            get
            {
                if (string.IsNullOrEmpty(_S3Path)) return "root";
                if (_S3Path.EndsWith("/")) return "path";
                return "file";
            }
        }

        private S3TreeItem _Parent;

        public S3TreeItem Parent
        {
            get => _Parent;
            set
            {
                if (_Parent != value)
                {
                    _Parent = value;
                    PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Parent)));
                }
            }
        }

        public string PathFromRoot()
        {
            string path = "";
            S3TreeItem node = this;
            do
            {
                path = node.S3Path + path;
                node = node.Parent;
            }
            while (node != null);
            return path;
        }

        public ObservableCollection<S3TreeItem> Items { get; set; } = new ObservableCollection<S3TreeItem>();

        public event PropertyChangedEventHandler PropertyChanged;

        public virtual async Task Load() { }
    }

    public class S3TreeRoot : S3TreeItem
    {
        public S3TreeRoot(AWSS3API awsS3API) : base(awsS3API)
        {
            Title = awsS3API.Options.Bucket;
        }

        public override async Task Load()
        {
            var paths = await awsS3API.ListPaths("", "/");
            Items.Clear();
            foreach (var path in paths)
            {
                var s3Path = new S3TreePath(awsS3API)
                {
                    S3Path = path,
                    Title = path.Replace("/", ""),
                    Parent = this,
                };
                Items.Add(s3Path);
                await s3Path.Load();
            }
            var files = await awsS3API.ListFiles("", "/");
            foreach (var file in files)
            {
                Items.Add(new S3TreeFile(awsS3API)
                {
                    S3Path = file,
                    Title = file,
                    Parent = this,
                });
            }
        }
    }
    public class S3TreePath : S3TreeItem
    {
        public S3TreePath(AWSS3API awsS3API) : base(awsS3API)
        {
        }

        public override async Task Load()
        {
            var paths = await awsS3API.ListPaths(S3Path, "/");
            Items.Clear();
            foreach (var path in paths)
            {
                Items.Add(new S3TreePath(awsS3API)
                {
                    S3Path = path,
                    Title = path.Replace("/", ""),
                    Parent = this,
                });
            }
            var files = await awsS3API.ListFiles(S3Path, "/");
            foreach (var file in files)
            {
                if (string.IsNullOrWhiteSpace(file)) continue;
                Items.Add(new S3TreeFile(awsS3API)
                {
                    S3Path = file,
                    Title = file,
                    Parent = this
                });
            }
        }

    }
    public class S3TreeFile : S3TreeItem
    {
        public S3TreeFile(AWSS3API awsS3API) : base(awsS3API)
        {
        }

        public FormatedQuery Query { get; set; }

        public override async Task Load()
        { 
            if(Query == null)
            {
                Query = await awsS3API.ReadFromJson<FormatedQuery>(PathFromRoot());
                Query.S3Path = PathFromRoot();
            }
        }
    }
}
