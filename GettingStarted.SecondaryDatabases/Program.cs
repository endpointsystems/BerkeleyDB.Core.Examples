using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using CsvHelper;

using static System.Console;
namespace GettingStarted.SecondaryDatabases
{
  class Program
  {
    static void Main()
    {

      //this value was set in the project settings. We'll read CSVs and write DB files there.
      var dataPath = Environment.GetEnvironmentVariable("DATA_DIR");
      var vendorRepo = new VendorRepository(dataPath);
      var invRepo = new InventoryRepository(dataPath);

      if (vendorRepo.RecordCount() == 0 || invRepo.RecordCount() == 0)
      {
        ImportData(dataPath,vendorRepo,invRepo);
      }

      //iterate through the vendor list
      //Console.WriteLine("iterating through cursor...");
      //vendorRepo.IterateThroughCursor();
      vendorRepo.FindKey("Endpoint Systems");

      var vendor = new Vendor
      {
        City = "Boynton Beach",
        PhoneNumber = "866-637-7274",
        SalesRep = "Lucas Vogel",
        SalesRepPhone = "866-637-7274",
        State = "Florida",
        Street = "2500 Quantum Lakes Dr",
        VendorName = "Endpoint Systems",
        Zip = "33426"
      };

      vendorRepo.AddVendor(vendor);
      vendorRepo.UpdateVendor(vendor);
      vendorRepo.DeleteRecordUsingCursor("Endpoint Systems");

      WriteLine("Press any key to exit.");

      vendorRepo.Dispose();
      invRepo.Dispose();

      ReadLine();
    }

    private static void ImportData(string dataPath, VendorRepository vendorRepo, InventoryRepository invRepo)
    {
      var commitTimes = new List<long>();
      var readTimer = new Stopwatch();
      readTimer.Start();
      var vr = new CsvReader(File.OpenText(Path.Combine(dataPath, "vendor-master.csv")));
      vr.Configuration.HasHeaderRecord = false;
      vr.Configuration.RegisterClassMap<VendorMap>();
      vr.Configuration.DetectColumnCountChanges = true;
      var ir = new CsvReader(File.OpenText(Path.Combine(dataPath, "inv-master.csv")));
      ir.Configuration.HasHeaderRecord = false;
      ir.Configuration.DetectColumnCountChanges = true;

      int z = 0;
      var writeTimer = new Stopwatch();
      Vendor vendor;
      Inventory inv;
      while (vr.Read())
      {
        vendor = vr.GetRecord<Vendor>();

        if (vendorRepo.FindKey(vendor.VendorName) > 0) continue;

        writeTimer.Start();
        vendorRepo.AddVendor(vendor);

        int y = z + 5;
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
            WriteLine($"{DateTime.Now}\tno more inventory");
          }
        }
        z = y;
        writeTimer.Stop();
        commitTimes.Add(writeTimer.ElapsedTicks);
        writeTimer.Reset();
      }


      readTimer.Stop();
      WriteLine("-----------------------------");
      WriteLine();
      WriteLine($"Read of CSV files took {readTimer.Elapsed.Minutes}:{readTimer.Elapsed.Seconds}.{readTimer.Elapsed.Milliseconds}.");
      readTimer.Reset();
      readTimer.Start();
      vendorRepo.Sync();
      invRepo.Sync();
      readTimer.Stop();
      WriteLine($"syncing databases took {readTimer.ElapsedMilliseconds} milliseconds.");
      WriteLine();
      WriteLine($" Average write for each iteration (1 vendor, 5 inventory) averaged {commitTimes.Average()} ticks. Min: {commitTimes.Min()} Max: {commitTimes.Max()}");

    }
  }
}
