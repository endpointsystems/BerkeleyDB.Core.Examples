using System;
using GettingStarted.DataWriting;
using Microsoft.Extensions.DependencyInjection;

namespace GettingStarted.Btree.Partitioning
{
    class Program
    {
        static void Main(string[] args)
        {
            var services = new ServiceCollection()
                .AddLogging();

            if (String.IsNullOrEmpty(Environment.GetEnvironmentVariable("DATA_DIR")))
                throw new ApplicationException("DATA_DIR environment must be set to directory containing example CSV files.");
          
            services.AddSingleton<IVendorRepository, VendorRepository>();
            services.AddSingleton<IInventoryRepository, InventoryRepository>();
            
            var app = new Application(services);
            app.Run();
            //var vr = app.Services.GetService(typeof(IVendorRepository)) as VendorRepository;
            //var ir = app.Services.GetService(typeof(IInventoryRepository)) as InventoryRepository;
            //var loggerFactory = app.Services.GetService<ILoggerFactory>();
            
            //var log = loggerFactory.CreateLogger("summary");
            
            
            //log.LogInformation("Inventory database stats: ");
            //ir?.PrintStats();

            //log.LogInformation("Vendor database stats: ");
            //vr?.PrintStats();

            Console.ReadLine();
        }
    }
}
