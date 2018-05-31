using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using BerkeleyDB.Core;
using GettingStarted.DataWriting;
using Microsoft.Extensions.Logging;

namespace GettingStarted.SecondaryDatabases
{
    public abstract class Repository : IDisposable
    {
        protected string path;
        protected BTreeDatabase db;
        protected SecondaryBTreeDatabase indexDb;
        protected BTreeCursor cursor;
        protected SecondaryCursor scursor;
        protected ILogger logger;
        protected Repository(string databaseName, ILoggerFactory loggerFactory)
        {
            path = Environment.GetEnvironmentVariable("DATA_DIR");
            logger = loggerFactory.CreateLogger(databaseName);
            var cfg = new BTreeDatabaseConfig
            {
                Creation = CreatePolicy.IF_NEEDED,
                CacheSize = new CacheInfo(1, 0, 1),
                ErrorFeedback = (prefix, message) =>
                {
                    logger.LogError($"{DateTime.Now} [primary] {prefix}: {message}");
                },
                ErrorPrefix = databaseName
            };
            db = BTreeDatabase.Open(Path.Combine(path, databaseName + ".db"), cfg);

            //set up secondary database
            var scfg = new SecondaryBTreeDatabaseConfig(db, GenerateIndex)
            {
                Creation = CreatePolicy.IF_NEEDED,
                Duplicates = DuplicatesPolicy.SORTED,
                ErrorFeedback = (prefix, message) =>
                {
                    logger.LogError($"{DateTime.Now} [secondary] {prefix}: {message}");
                }
            };

            indexDb = SecondaryBTreeDatabase.Open(Path.Combine(path, databaseName + "-index.db"), scfg);

            cursor = db.Cursor();
            scursor = indexDb.SecondaryCursor();
        }

        protected virtual DatabaseEntry GenerateIndex(DatabaseEntry key, DatabaseEntry value)
        {
            logger.LogInformation("calling null secondary function...");
            return null;
        }

        public int FindKey(string keyValue)
        {
            var keypair = (from KeyValuePair<DatabaseEntry, KeyValuePair<DatabaseEntry, DatabaseEntry>> s in scursor
                           where Encoding.UTF8.GetString(s.Key.Data).Contains(keyValue)
                           select s).ToList();
            return keypair.Count;
        }

        public void AddItem(DatabaseEntry key, DatabaseEntry value)
        {
            cursor.Add(new KeyValuePair<DatabaseEntry, DatabaseEntry>(key, value));
        }

        public long RecordCount()
        {
            return cursor.LongCount();
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
            try
            {
                var key = new DatabaseEntry(Encoding.UTF8.GetBytes(keyval));
                var data = new DatabaseEntry(dataval);
                db.Put(key, data);
            }
            catch (Exception ex)
            {
                Sync();
                logger.LogError($"{DateTime.Now}\t{ex.GetType()} in {db.DatabaseName} AddToDb: {ex.Message}");
            }
        }

        /// <summary>
        /// Add records to the database through the cursor.
        /// </summary>
        /// <param name="keyval">The key.</param>
        /// <param name="dataval">The data value.</param>
        protected void AddToCursor(string keyval, byte[] dataval)
        {
            //try
            //{
            var key = new DatabaseEntry(Encoding.UTF8.GetBytes(keyval));
            var data = new DatabaseEntry(dataval);
            cursor.Add(new KeyValuePair<DatabaseEntry, DatabaseEntry>(key, data));
            //}
            //catch (Exception ex)
            //{
            //  WriteLine($"{DateTime.Now}\tcannot write {keyval}: {ex.Message}");
            //}
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
            indexDb.Sync();
        }

        private void Dispose(bool disposing)
        {
            if (!disposing) return;
            cursor?.Close();
            indexDb?.Close(true);
            indexDb?.Dispose();
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
        public VendorRepository(ILoggerFactory loggerFactory) : base("vendor", loggerFactory) { }
        public void AddVendor(Vendor v) { AddToDb(v.VendorId.ToString(), v.ToByteArray()); }
        public void Save()
        {
            Sync();
        }

        public void UpdateVendor(Vendor v) { ModifyRecordUsingCursor(v.VendorName, v.ToByteArray()); }

        protected override DatabaseEntry GenerateIndex(DatabaseEntry key, DatabaseEntry value)
        {
            var vendor = Vendor.Deserialize(value.Data);
            //WriteLine($"sales rep: {vendor.SalesRep}");
            return new DatabaseEntry(vendor.SalesRep.ToByteArray());
        }
    }

    /// <summary>
    /// The repository for our inventory database.
    /// </summary>
    public class InventoryRepository : Repository, IInventoryRepository
    {
        public InventoryRepository(ILoggerFactory loggerFactory) : base( "inventory", loggerFactory) { }
        public void AddInventory(string sku, Inventory inv) { AddToDb(sku, inv.ToByteArray()); }
        public void Save()
        {
            Sync();
        }

        protected override DatabaseEntry GenerateIndex(DatabaseEntry key, DatabaseEntry value)
        {
            var inv = Inventory.Deserialize(value.Data);
            //WriteLine($"vendor: {inv.Vendor}");
            return new DatabaseEntry(inv.Vendor.ToByteArray());
        }
    }



}
