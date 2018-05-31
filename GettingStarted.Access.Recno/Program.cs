using System;
using GettingStarted.DataWriting;
using Microsoft.Extensions.DependencyInjection;

namespace GettingStarted.Access.Recno
{
    class Program
    {
        static void Main()
        {
            var services = new ServiceCollection()
                .AddLogging();

          
            //this value was set in the project settings. the value's pulled in the parent repository constructor.
            //var dataPath = Environment.GetEnvironmentVariable("DATA_DIR");

            services.AddSingleton<IVendorRepository, VendorRepository>();
            services.AddSingleton<IInventoryRepository, InventoryRepository>();
            
            var app = new Application(services);
            app.Run();

            Console.ReadLine();
        }
    }
}
