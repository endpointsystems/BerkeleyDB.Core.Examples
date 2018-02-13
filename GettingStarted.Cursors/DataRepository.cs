using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using BerkeleyDB.Core;

namespace GettingStarted.Cursors
{
  public abstract class Repository: IDisposable
  {
    protected string path;
    protected BTreeDatabase db;
    protected BTreeCursor cursor;

    protected Repository(string dataPath, string databaseName)
    {
      path = dataPath;
      var cfg = new BTreeDatabaseConfig
      {
        Creation = CreatePolicy.IF_NEEDED, CacheSize = new CacheInfo(1, 0, 1),
        ErrorFeedback = (prefix, message) =>
        {
          var fg = Console.ForegroundColor;
          Console.ForegroundColor = ConsoleColor.Red;
          Console.WriteLine($"{prefix}: {message}");
          Console.ForegroundColor = fg;
        },
        ErrorPrefix = databaseName,Duplicates = DuplicatesPolicy.SORTED,        
      };
      db = BTreeDatabase.Open(Path.Combine(dataPath,databaseName +".db"),cfg);
      
      cursor = db.Cursor();
    }

    public void IterateThroughCursor()
    {
      var watch = new Stopwatch();
      watch.Start();
      foreach (var pair in cursor)
      {
        Console.WriteLine(Encoding.UTF8.GetString(pair.Key.Data));
      }
      watch.Stop();
      Console.WriteLine($"task completed in {watch.ElapsedMilliseconds} milliseconds.");
      Console.WriteLine("---");
    }

    public void FindKey(string keyValue)
    {
      var pairs = (from c in cursor where Encoding.UTF8.GetString(c.Key.Data).Contains(keyValue) select c).ToList();
      var watch = new Stopwatch();
      watch.Start();
      foreach (var pair in pairs)
      {
        Console.WriteLine($"\t{Encoding.UTF8.GetString(pair.Key.Data)}");
      }
      watch.Stop();
      Console.WriteLine($"task completed in {watch.ElapsedMilliseconds} milliseconds.");
      Console.WriteLine($"found {pairs.Count} records.");      
      Console.WriteLine("---");
    }

    public void AddItem(DatabaseEntry key, DatabaseEntry value)
    {      
      cursor.Add(new KeyValuePair<DatabaseEntry, DatabaseEntry>(key,value));
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
      cursor.Add(new KeyValuePair<DatabaseEntry, DatabaseEntry>(key,data));      
    }

    protected void AddUniqueToCursor(string keyval, byte[] dataval)
    {
      var key = new DatabaseEntry(Encoding.UTF8.GetBytes(keyval));
      var data = new DatabaseEntry(dataval);
      cursor.AddUnique(new KeyValuePair<DatabaseEntry, DatabaseEntry>(key,data));      
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
          Console.WriteLine($"key found: '{keyval}'. Deleting...");          
          cursor.Delete();          
          Console.WriteLine($"database record '{keyval}' deleted.");          
          break;
        }

        b = cursor.MoveNext();
      }
      watch.Stop();
      Console.WriteLine($"delete task completed in {watch.ElapsedMilliseconds} milliseconds.");
      Console.WriteLine("---");
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
          Console.WriteLine($"key found: '{keyval}'. Modifying...");
          cursor.Current.Value.Data = dataval;          
          Console.WriteLine($"database record '{keyval}' modified.");          
          break;
        }

        b = cursor.MoveNext();
      }
      watch.Stop();
      Console.WriteLine($"modify task completed in {watch.ElapsedMilliseconds} milliseconds.");
      Console.WriteLine("---");
    }
    
    public void Sync()
    {
      Console.WriteLine("I'm syncing!");
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
  public class VendorRepository: Repository
  {
    public VendorRepository(string dataPath) : base(dataPath, "vendor"){}
    public void AddVendor(Vendor v){AddToCursor(v.VendorName,v.ToByteArray());}
    public void UpdateVendor(Vendor v){ModifyRecordUsingCursor(v.VendorName,v.ToByteArray());}
  }

  /// <summary>
  /// The repository for our inventory database.
  /// </summary>
  public class InventoryRepository: Repository
  {
    public InventoryRepository(string dataPath) : base(dataPath,"inventory"){}
    public void AddInventory(string sku, Inventory inv){AddToDb(sku,inv.ToByteArray());}
  }
}
