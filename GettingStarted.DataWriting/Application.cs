using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace GettingStarted.DataWriting
{
    public class Application
    {
        public IServiceProvider Services { get; set; }

        public Application(IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<ICsvReader,CsvReader>();
            Services = serviceCollection.BuildServiceProvider();
            Services.GetService<ILoggerFactory>().AddConsole();
        }

        public void Run()
        {
            Services.GetService<ICsvReader>().UploadData();
        }
    }
}
