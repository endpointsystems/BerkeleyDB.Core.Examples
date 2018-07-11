using System;
using GettingStarted.DataWriting;
using Microsoft.Extensions.DependencyInjection;

namespace GettingStarted.Access.Heap
{
    class Program
    {
        static void Main()
        {
            var services = new ServiceCollection()
                .AddLogging();

          
            if (String.IsNullOrEmpty(Environment.GetEnvironmentVariable("DATA_DIR")))
                throw new ApplicationException("DATA_DIR environment must be set to directory containing example CSV files.");

            services.AddSingleton<IVendorRepository, VendorRepository>();
            services.AddSingleton<IInventoryRepository, InventoryRepository>();
            
            var app = new Application(services);
            app.Run();

            Console.ReadLine();
        }
    }
}
