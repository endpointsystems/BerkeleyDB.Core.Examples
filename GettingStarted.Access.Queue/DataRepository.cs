﻿using System;
using System.IO;
using System.Text;
using BerkeleyDB.Core;
using GettingStarted.DataWriting;
using Microsoft.Extensions.Logging;

namespace GettingStarted.Access.Queue
{
    public abstract class Repository : IDisposable
    {
        protected string path;
        protected QueueDatabase db;
        protected ILogger logger;
        protected Repository(string databaseName, uint tableSize, ILoggerFactory loggerService)
        {
            logger = loggerService.CreateLogger(databaseName);
            path = Environment.GetEnvironmentVariable("DATA_DIR");
            var cfg = new QueueDatabaseConfig
            {
                Creation = CreatePolicy.IF_NEEDED,
                //CacheSize = new CacheInfo(1, 0, 1),
                ErrorFeedback = (prefix, message) =>
                {
                    logger.LogCritical($"{prefix}: {message}");
                },
                ErrorPrefix = databaseName,                
                //This must be in place - and be equal to or larger than your data record size (but less than the page size which is 4k) - or you'll get errors.
                Length =500,

                
                //Duplicates = DuplicatesPolicy.UNSORTED,
                //TableSize = tableSize
            };
            
            db = QueueDatabase.Open(Path.Combine(path,databaseName +".db"),cfg);
            
        }


        ~Repository()
        {
            Dispose(false);
        }

        protected void AddToDb(string keyval, byte[] dataval)
        {
            var key = new DatabaseEntry(Encoding.UTF8.GetBytes(keyval));
            var data = new DatabaseEntry(dataval);
            //This is the data insertion method for Queue/Recno databases.
            db.Append(data);
            //...not this one
            //db.Put(key, data);
        }

        public void Sync()
        {
            logger.LogInformation("I'm syncing!");
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
    public class VendorRepository : Repository, IVendorRepository
    {
        private int i;
        public VendorRepository(ILoggerFactory logger) : base("vendor",8,logger)
        {
        }

        public void AddVendor(Vendor v)
        {
            i++;
            v.VendorId = i;
            AddToDb(i.ToString(),v.ToByteArray());
        }

        public void Save()
        {
            Sync();
        }
    }

    /// <summary>
    /// The repository for our inventory database.
    /// </summary>
    public class InventoryRepository : Repository, IInventoryRepository
    {
        public InventoryRepository(ILoggerFactory logger) : base("inventory",5, logger) { }
        public void AddInventory(string sku, Inventory inv) { AddToDb(sku, inv.ToByteArray()); }
        public void Save()
        {
            Sync();
        }
    }
}
