using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using BerkeleyDB.Core;

namespace GettingStarted.Access.Heap
{
    public abstract class Repository : IDisposable
    {
        protected string path;
        protected HeapDatabase db;
        protected DatabaseEnvironment env;
        protected Repository(string dataPath, string databaseName)
        {
            path = dataPath;
            var cfg = new HeapDatabaseConfig()
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

            db = HeapDatabase.Open(Path.Combine(dataPath,databaseName +".db"),cfg);
            
        }


        ~Repository()
        {
            Dispose(false);
        }

        protected void AddToDb(string keyval, byte[] dataval)
        {
            var key = new DatabaseEntry(Encoding.UTF8.GetBytes(keyval));
            var data = new DatabaseEntry(dataval);
            db.Put(key, data);
        }

        public void Sync()
        {
            Console.WriteLine("I'm syncing!");
            db.Sync();
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
    }
    /// <summary>
    /// The data repository for our Vendor database.
    /// </summary>
    public class VendorRepository : Repository
    {

        public VendorRepository(string dataPath) : base(dataPath, "vendor")
        {
        }

        public void AddVendor(Vendor v)
        {
            db.Append(new DatabaseEntry(v.ToByteArray()));
            //keyList.Add(new DatabaseEntry(v.VendorName.ToByteArray()));
            //valueList.Add(new DatabaseEntry(v.ToByteArray()));
        }

    }

    /// <summary>
    /// The repository for our inventory database.
    /// </summary>
    public class InventoryRepository : Repository
    {
        public InventoryRepository(string dataPath) : base(dataPath, "inventory") { }

        public void AddInventory(string sku, Inventory inv)
        {
            db.Append(new DatabaseEntry(inv.ToByteArray()));
        }
    }
}
