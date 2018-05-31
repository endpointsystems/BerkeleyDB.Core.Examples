using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace GettingStarted.DataWriting
{
    /// <summary>
    /// Use this class to read the CSV files into the repositories.
    /// </summary>
    public class CsvReader: ICsvReader
    {
        private readonly IVendorRepository vendorRepo;
        private readonly IInventoryRepository invRepo;
        private readonly string dataPath;
        private readonly ILogger log;
        public CsvReader(IVendorRepository vendorRepoRepository, IInventoryRepository invRepoRepository, ILoggerFactory logger)
        {
            log = logger.CreateLogger("CsvReader");
            vendorRepo = vendorRepoRepository;
            invRepo = invRepoRepository;
            //this value was set in the project settings. We'll read CSVs and write DB files there.
            dataPath = Environment.GetEnvironmentVariable("DATA_DIR");
        }
        /// <summary>
        /// Upload the data into the databases.
        /// </summary>
        public void UploadData()
        {
            List<long> commitTimes = new List<long>();
            var readTimer = new Stopwatch();
            readTimer.Start();
            var vr = new CsvHelper.CsvReader(File.OpenText(Path.Combine(dataPath, "vendor-master.csv")));
            vr.Configuration.HasHeaderRecord = false;
            vr.Configuration.RegisterClassMap<VendorMap>();
            vr.Configuration.DetectColumnCountChanges = true;
            var ir = new CsvHelper.CsvReader(File.OpenText(Path.Combine(dataPath, "inv-master.csv")));
            ir.Configuration.HasHeaderRecord = false;
            ir.Configuration.DetectColumnCountChanges = true;

            int z = 0;
            var writeTimer = new Stopwatch();
            Vendor vendor;
            Inventory inv;
            while (vr.Read())
            {
                writeTimer.Start();
                int y = z + 5;
                vendor = vr.GetRecord<Vendor>();
                vendorRepo.AddVendor(vendor);

                for (int x = z; x < y; x++)
                {
                    if (ir.Read())
                    {
                        inv = new Inventory(vendor.VendorName)
                        {
                            Category = ir.GetField(2),
                            Name = ir.GetField(3),
                            Price = ir.GetField<double>(0),
                            Quantity = ir.GetField<int>(1)
                        };
                        invRepo.AddInventory(ir.GetField(5), inv);
                    }
                    else
                    {
                        log.LogCritical($"{DateTime.Now}\tno more inventory");
                    }
                }
                z = y;
                writeTimer.Stop();
                commitTimes.Add(writeTimer.ElapsedTicks);
                writeTimer.Reset();
            }

            vendorRepo.Save();
            readTimer.Stop();
            log.LogInformation("-----------------------------");
            log.LogInformation("");
            log.LogInformation("");
            log.LogInformation($"Read of CSV files took {readTimer.Elapsed.Minutes}:{readTimer.Elapsed.Seconds}.{readTimer.Elapsed.Milliseconds}.");
            readTimer.Reset();
            readTimer.Start();
            vendorRepo.Save();
            invRepo.Save();
            readTimer.Stop();
            log.LogInformation($"syncing databases took {readTimer.ElapsedMilliseconds} milliseconds.");
            log.LogInformation("");
            log.LogInformation($" Average write for each iteration (1 vendor, 5 inventory) averaged {commitTimes.Average()} ticks. Min: {commitTimes.Min()} Max: {commitTimes.Max()}");
        }


    }
}
