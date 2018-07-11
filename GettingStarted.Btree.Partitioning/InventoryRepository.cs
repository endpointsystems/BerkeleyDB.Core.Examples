using System;
using System.IO;
using System.Text;
using BerkeleyDB;
using GettingStarted.DataWriting;
using Microsoft.Extensions.Logging;

namespace GettingStarted.Btree.Partitioning
{
    public class InventoryRepository: IDisposable, IInventoryRepository
    {
        protected string path;
        protected BTreeDatabase db;
        protected ILogger logger;

        public InventoryRepository(ILoggerFactory loggerService)
        {
            var databaseName = "inventory";
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

            // try to partition equally among the alphabet for the SKU keys
            cfg.SetPartitionByKeys(new[] {new DatabaseEntry(BitConverter.GetBytes('I')),new DatabaseEntry(BitConverter.GetBytes('R')), });
            db = BTreeDatabase.Open(Path.Combine(path,databaseName +".db"),cfg);
            
        }

        ~InventoryRepository()

        {
            Dispose(false);
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

        protected void AddToDb(string keyval, byte[] dataval)
        {
            var key = new DatabaseEntry(Encoding.UTF8.GetBytes(keyval));
            var data = new DatabaseEntry(dataval);
            db.Put(key, data);
        }

        public void PrintStats()
        {
            db.PrintStats(true);
        }


        public void AddInventory(string sku, Inventory inv)
        {
            AddToDb(sku,inv.ToByteArray());
        }

        public void Save()
        {
            logger.LogInformation("I'm syncing!");
            db.Sync();
        }
    }
}
