using Autofac;

namespace Jack.DataScience.Scrapping
{
    public class ScrapingModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.Register(context =>
            {
                return new ScrapingEngine(context.Resolve<IComponentContext>());
            });
        }
    }
}
