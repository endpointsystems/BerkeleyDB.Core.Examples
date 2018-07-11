using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using BerkeleyDB;
using GettingStarted.DataWriting;
using Microsoft.Extensions.Logging;

namespace GettingStarted.Cursors
{
    public abstract class Repository : IDisposable
    {
        protected string path;
        protected BTreeDatabase db;
        protected BTreeCursor cursor;
        protected ILogger logger;
        protected Repository(string databaseName, ILoggerFactory loggerService)
        {
            logger = loggerService.CreateLogger(databaseName);
            path = Environment.GetEnvironmentVariable("DATA_DIR");
            var cfg = new BTreeDatabaseConfig
            {
                Creation = CreatePolicy.IF_NEEDED,
                CacheSize = new CacheInfo(1, 0, 1),
                ErrorFeedback = (prefix, message) =>
                {
                    logger.LogError($"{prefix}: {message}");
                },
                ErrorPrefix = databaseName,
                Duplicates = DuplicatesPolicy.SORTED,
            };
            db = BTreeDatabase.Open(Path.Combine(path, databaseName + ".db"), cfg);

            cursor = db.Cursor();
        }

        public void IterateThroughCursor()
        {
            var watch = new Stopwatch();
            watch.Start();
            foreach (var pair in cursor)
            {
                logger.LogInformation(Encoding.UTF8.GetString(pair.Key.Data));
            }
            watch.Stop();
            logger.LogInformation($"task completed in {watch.ElapsedMilliseconds} milliseconds.");
            logger.LogInformation("---");
        }

        public void FindKey(string keyValue)
        {
            var pairs = (from c in cursor where Encoding.UTF8.GetString(c.Key.Data).Contains(keyValue) select c).ToList();
            var watch = new Stopwatch();
            watch.Start();
            foreach (var pair in pairs)
            {
                logger.LogInformation($"\t{Encoding.UTF8.GetString(pair.Key.Data)}");
            }
            watch.Stop();
            logger.LogInformation($"task completed in {watch.ElapsedMilliseconds} milliseconds.");
            logger.LogInformation($"found {pairs.Count} records.");
            logger.LogInformation("---");
        }

        public void AddItem(DatabaseEntry key, DatabaseEntry value)
        {
            cursor.Add(new KeyValuePair<DatabaseEntry, DatabaseEntry>(key, value));
        }

        ~Repository()
        {
            Dispose(false);
        }

        /// <summary>
        /// Add records directly to the database.
        /// </summary>
        /// <param name="keyval">The key.</param>
        /// <param name="dataval">The data value.</param>
        protected void AddToDb(string keyval, byte[] dataval)
        {
            var key = new DatabaseEntry(Encoding.UTF8.GetBytes(keyval));
            var data = new DatabaseEntry(dataval);
            db.Put(key, data);
        }

        /// <summary>
        /// Add records to the database through the cursor.
        /// </summary>
        /// <param name="keyval">The key.</param>
        /// <param name="dataval">The data value.</param>
        protected void AddToCursor(string keyval, byte[] dataval)
        {
            var key = new DatabaseEntry(Encoding.UTF8.GetBytes(keyval));
            var data = new DatabaseEntry(dataval);
            cursor.Add(new KeyValuePair<DatabaseEntry, DatabaseEntry>(key, data));
        }

        protected void AddUniqueToCursor(string keyval, byte[] dataval)
        {
            var key = new DatabaseEntry(Encoding.UTF8.GetBytes(keyval));
            var data = new DatabaseEntry(dataval);
            cursor.AddUnique(new KeyValuePair<DatabaseEntry, DatabaseEntry>(key, data));
        }

        public void DeleteRecordUsingCursor(string keyval)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();
            bool b = true;
            cursor.MoveFirst();
            while (b)
            {
                if (Encoding.UTF8.GetString(cursor.Current.Key.Data) == keyval)
                {
                    logger.LogInformation($"key found: '{keyval}'. Deleting...");
                    cursor.Delete();
                    logger.LogInformation($"database record '{keyval}' deleted.");
                    break;
                }

                b = cursor.MoveNext();
            }
            watch.Stop();
            logger.LogInformation($"delete task completed in {watch.ElapsedMilliseconds} milliseconds.");
            logger.LogInformation("---");
        }

        protected void ModifyRecordUsingCursor(string keyval, byte[] dataval)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();
            bool b = true;
            cursor.MoveFirst();
            while (b)
            {
                if (Encoding.UTF8.GetString(cursor.Current.Key.Data) == keyval)
                {
                    logger.LogInformation($"key found: '{keyval}'. Modifying...");
                    cursor.Current.Value.Data = dataval;
                    logger.LogInformation($"database record '{keyval}' modified.");
                    break;
                }

                b = cursor.MoveNext();
            }
            watch.Stop();
            logger.LogInformation($"modify task completed in {watch.ElapsedMilliseconds} milliseconds.");
            logger.LogInformation("---");
        }

        public void Sync()
        {
            logger.LogInformation("I'm syncing!");
            db.Sync();
        }

        private void Dispose(bool disposing)
        {
            if (!disposing) return;
            cursor?.Close();
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
    public class VendorRepository : Repository, IVendorRepository
    {
        public VendorRepository(ILoggerFactory logger) : base( "vendor",logger) { }
        public void AddVendor(Vendor v) { AddToCursor(v.VendorName, v.ToByteArray()); }
        public void Save()
        {
            Sync();
        }

        public void UpdateVendor(Vendor v) { ModifyRecordUsingCursor(v.VendorName, v.ToByteArray()); }
    }

    /// <summary>
    /// The repository for our inventory database.
    /// </summary>
    public class InventoryRepository : Repository, IInventoryRepository
    {
        public InventoryRepository(ILoggerFactory factory) : base("inventory", factory) { }
        public void AddInventory(string sku, Inventory inv) { AddToDb(sku, inv.ToByteArray()); }
        public void Save()
        {
            Sync();
        }
    }
}
