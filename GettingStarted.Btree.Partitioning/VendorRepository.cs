using System;
using System.IO;
using System.Text;
using BerkeleyDB;
using GettingStarted.DataWriting;
using Microsoft.Extensions.Logging;

/*
 * In this example, we're using two distinctly separate repository classes instead of having an abstracted base class. This allows us to
 * fine-tune our databases and database operations for Vendor and Inventory.
 */
namespace GettingStarted.Btree.Partitioning
{
    public class VendorRepository : IDisposable, IVendorRepository
    {
        protected string path;
        protected BTreeDatabase db;
        protected ILogger logger;
        protected int vendorId;
        
        public  VendorRepository(ILoggerFactory loggerService)
        {
            var databaseName = "vendor";
            path = Environment.GetEnvironmentVariable("DATA_DIR");
            logger = loggerService.CreateLogger(databaseName);
            var cfg = new BTreeDatabaseConfig
            {
                
                Creation = CreatePolicy.IF_NEEDED,
                CacheSize = new CacheInfo(1, 0, 1),
                ErrorFeedback = (prefix, message) =>
                {
                    var fg = Console.ForegroundColor;
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"{prefix}: {message}");
                    Console.ForegroundColor = fg;
                },
                ErrorPrefix = databaseName,                
                
                //Duplicates = DuplicatesPolicy.SORTED,
                //TableSize = tableSize
            };           
            // we have 50k vendors, so we're going to split into two equal partitions of 25k
            cfg.SetPartitionByKeys(new[] {new DatabaseEntry(BitConverter.GetBytes(25000))});
            cfg.SetPartitionByCallback(3, key => { return 0; });
            db = BTreeDatabase.Open(Path.Combine(path,databaseName +".db"),cfg);
            BTreeDatabase.Verify("",cfg,Database.VerifyOperation.DEFAULT);
        }
        
        public void PrintStats()
        {
            db.PrintStats(true);
            db.PrintFastStats();
        }

        ~VendorRepository()
        {
            Dispose(false);
        }

        protected void AddToDb(string keyval, byte[] dataval)
        {
            var key = new DatabaseEntry(Encoding.UTF8.GetBytes(keyval));
            var data = new DatabaseEntry(dataval);
            db.Put(key, data);
        }

        protected void AddToDb(int keyval, byte[] dataval)
        {
            var key = new DatabaseEntry(BitConverter.GetBytes(keyval));
            var data = new DatabaseEntry(dataval);
            db.Put(key, data);
        }

        private void Dispose(bool disposing)
        {
            if (!disposing) return;
            db?.Close(true);
            db?.Dispose();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void AddVendor(Vendor v)
        {
            vendorId++;
            v.VendorId = vendorId;
            AddToDb(vendorId,v.ToByteArray());
        }

        public void Save()
        {
            logger.LogInformation("I'm syncing!");
            db.Sync();
        }
    }

}
