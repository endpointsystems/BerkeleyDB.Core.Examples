using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using CsvHelper.Configuration;

namespace GettingStarted.SecondaryDatabases
{
    [Serializable]
    public class Vendor
    {
      public string VendorName;
      public string Street;
      public string City;
      public string State;
      public string Zip;
      public string PhoneNumber;
      public string SalesRep;
      public string SalesRepPhone;

      public static Vendor Deserialize(byte[] data)
      {
        BinaryFormatter formatter = new BinaryFormatter();
        using (var ms = new MemoryStream(data))
        {
          return formatter.Deserialize(ms) as Vendor;
        }
      }
    }

  public class VendorMap : ClassMap<Vendor>
  {
    public VendorMap()
    {
      Map(m => m.VendorName);
      Map(m => m.Street);
      Map(m => m.City);
      Map(m => m.State);
      Map(m => m.Zip);
      Map(m => m.PhoneNumber);
      Map(m => m.SalesRep);
      Map(m => m.SalesRepPhone);
    }
  }

  [Serializable]
  public class Inventory
  {
    public Inventory(string vendor)
    {
      Vendor = vendor;
    }

    public double Price;
    public int Quantity;
    public string Name;
    //this will also be our key, so we won't store it here
    //public string Sku;
    public string Category;
    public string Vendor;

    public static Inventory Deserialize(byte[] data)
    {
      BinaryFormatter formatter = new BinaryFormatter();
      using (var ms = new MemoryStream(data))
      {
        return formatter.Deserialize(ms) as Inventory;
      }
    }
  }

  public static class Extensions
  {
    public static byte[] ToByteArray(this string value)
    {
      return Encoding.Default.GetBytes(value);
    }

    public static byte[] ToByteArray(this Vendor vendor)
    {
      BinaryFormatter formatter = new BinaryFormatter();
      using (var ms = new MemoryStream())
      {
        formatter.Serialize(ms,vendor);
        //ms.Seek(0, SeekOrigin.Begin);
        return ms.ToArray();
      }
    }
    public static byte[] ToByteArray(this Inventory inv)
    {
      BinaryFormatter formatter = new BinaryFormatter();
      using (var ms = new MemoryStream())
      {
        formatter.Serialize(ms,inv);
        ms.Seek(0, SeekOrigin.Begin);
        return ms.ToArray();
      }
    }
  }
}
