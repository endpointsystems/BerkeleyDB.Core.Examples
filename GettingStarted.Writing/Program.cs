using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Diagnostics;
using CsvHelper;
namespace GettingStarted
{
    class Program
    {
        static void Main(string[] args)
        {
          List<long> commitTimes = new List<long>();

          //this value was set in the project settings. We'll read CSVs and write DB files there.
          var dataPath = Environment.GetEnvironmentVariable("DATA_DIR");
          if (string.IsNullOrEmpty(dataPath))
            dataPath = @"C:\Repos\BerkeleyDb.Core.Examples\BerkeleyDb.Core.Examples\data";
          var vendorRepo = new VendorRepository(dataPath);
          var invRepo = new InventoryRepository(dataPath);
      
          var readTimer = new Stopwatch();
          readTimer.Start();
          var vr = new CsvReader(File.OpenText(Path.Combine(dataPath,"vendor-master.csv")));
          vr.Configuration.HasHeaderRecord = false;
          vr.Configuration.RegisterClassMap<VendorMap>();
          vr.Configuration.DetectColumnCountChanges = true;
          var ir = new CsvReader(File.OpenText(Path.Combine(dataPath,"inv-master.csv")));
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
            //int x = z + 1;
            for (int x = z; x < y; x++)
            {
              //Console.WriteLine($"x: {x}, y: {y}, z: {z}");
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
                Console.WriteLine($"{DateTime.Now}\tno more inventory");
              }
            }
            z = y;
            writeTimer.Stop();
            commitTimes.Add(writeTimer.ElapsedTicks);
            writeTimer.Reset();
          }


          readTimer.Stop();
          Console.WriteLine("-----------------------------");
          Console.WriteLine();
          Console.WriteLine($"Read of CSV files took {readTimer.Elapsed.Minutes}:{readTimer.Elapsed.Seconds}.{readTimer.Elapsed.Milliseconds}.");
          readTimer.Reset();
          readTimer.Start();
          vendorRepo.Sync();
          invRepo.Sync();
          readTimer.Stop();
          Console.WriteLine($"syncing databases took {readTimer.ElapsedMilliseconds} milliseconds.");
          Console.WriteLine();
          Console.WriteLine($" Average write for each iteration (1 vendor, 5 inventory) averaged {commitTimes.Average()} ticks. Min: {commitTimes.Min()} Max: {commitTimes.Max()}");
          Console.WriteLine("Press any key to exit.");

          vendorRepo.Dispose();
          invRepo.Dispose();

          Console.ReadLine();
        }
    }
}
