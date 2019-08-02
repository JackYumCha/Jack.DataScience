using Autofac;
using Jack.DataScience.Compute.AWSBatch;
using Jack.DataScience.Compute.AWSEC2;
using Jack.DataScience.Data.AWSDynamoDB;
using Jack.DataScience.MQ.AWSSQS;
using Jack.DataScience.Storage.AWSS3;

namespace Jack.DataScience.Scrapping
{
    public class ScrapingModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<ScriptJobScheduler>();
            builder.RegisterModule<AWSS3Module>();
            builder.RegisterModule<AWSDynamoDBModule>();
            builder.RegisterModule<AWSBatchModule>();
            builder.RegisterModule<AWSSQSModule>();
            builder.RegisterModule<AWSEC2Module>();
            builder.Register(context =>
            {
                return new ScrapingEngine(context.Resolve<IComponentContext>());
            });
        }
    }
}
