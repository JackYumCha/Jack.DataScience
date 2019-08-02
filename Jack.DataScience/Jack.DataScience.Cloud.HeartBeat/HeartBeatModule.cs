using Autofac;

namespace Jack.DataScience.Cloud.HeartBeat
{
    public class HeartBeatModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<HeartBeatAPI>();
        }
    }
}
