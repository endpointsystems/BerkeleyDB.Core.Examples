using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Linq;
using BerkeleyDB;
using static System.Console;

namespace GettingStarted.BDBPG.SecondaryIndex
{
    class Program
    {
        static void Main()
        {
            if (String.IsNullOrEmpty(Environment.GetEnvironmentVariable("DATA_DIR")))
                throw new ApplicationException("DATA_DIR must be set to the directory containing student data!");

            var dataPath = Environment.GetEnvironmentVariable("DATA_DIR");

            var config = new BTreeDatabaseConfig
            {                
                Creation = CreatePolicy.IF_NEEDED,
                CacheSize = new CacheInfo(1, 0, 1),
                ErrorFeedback = (prefix, message) =>
                {
                    var fg = ForegroundColor;
                    ForegroundColor = ConsoleColor.Red;
                    WriteLine($"{prefix}: {message}");
                    ForegroundColor = fg;
                },
                ErrorPrefix = "students",                
            };

            var db = BTreeDatabase.Open(Path.Combine(dataPath, "students.db"), "students",config);

            var cfg = new SecondaryBTreeDatabaseConfig(db, (key, data) =>
            {
                var last = Encoding.Default.GetString(data.Data).Split('#').Last().Replace("\0","");
                //Console.WriteLine($"adding {last} to index");
                return new DatabaseEntry(Encoding.Default.GetBytes(last));
            })
            {
                Creation = CreatePolicy.IF_NEEDED,Duplicates = DuplicatesPolicy.SORTED,
                ErrorFeedback = (prefix, message) => WriteLine($"{prefix}: {message}")

            };

            var dbIdx = SecondaryBTreeDatabase.Open(Path.Combine(dataPath, "students.db"), "studentsIdx",cfg);

            if (db.Stats().nKeys < 1)
            {
                foreach (var line in File.ReadAllLines(Path.Combine(dataPath, "students.csv")))
                {
                    var split = line.Split("#");
                    var key = new DatabaseEntry(Encoding.Default.GetBytes(split[0]));
                    var bytes = new List<byte>();

                    //Add first and last name delimited by the '#' still
                    bytes.AddRange(Encoding.Default.GetBytes(split[1]));
                    bytes.AddRange(BitConverter.GetBytes('#'));
                    bytes.AddRange(Encoding.Default.GetBytes(split[2]));
                    var dbval = new DatabaseEntry(bytes.ToArray());
                    db.Put(key, dbval);
                }

                db.Sync();
                dbIdx.Sync();
            }

            var dbKeys = db.Stats().nKeys;            
            /* Data upload complete. Now let's find and delete some records. */

            var idxCursor = dbIdx.Cursor();
            idxCursor.MoveFirst();

            int results = 0;

            //var results = from c in idxCursor where c.Key == barrera select c;
            do
            {
                if (Encoding.Default.GetString(idxCursor.Current.Key.Data) == "Barrera")
                {
                    results++;
                    WriteLine(Encoding.Default.GetString(idxCursor.Current.Value.Data));
                }
            } while (idxCursor.MoveNext());

            WriteLine($"found {results} results containing Barrera.");
            WriteLine();

            // delete everyone named Barrera


            var dbCursor = db.Cursor();
            dbCursor.MoveFirst();
            do
            {
                var ckey = Encoding.Default.GetString(dbCursor.Current.Key.Data);
                if (ckey != "994BE3B4") continue;
                WriteLine("bartlett found! deleting...");
                try
                {
                    var v = db.Get(dbCursor.Current.Key);
                    WriteLine($"{Encoding.Default.GetString(v.Key.Data)}");
                    db.Delete(dbCursor.Current.Key);
                }
                catch (Exception ex)
                {
                    WriteLine($"{ex.GetType()}: {ex.Message}");
                }

                break;

            } while (dbCursor.MoveNext());

            string lastName = string.Empty;
            idxCursor.MoveFirst();
            do
            {
                var key = Encoding.Default.GetString(idxCursor.Current.Key.Data).Trim();
                if (lastName != key) lastName = key;
                if (key == "Barrera")
                {
                    try
                    {
                        WriteLine($"deleting '{key}'");
                        dbIdx.Delete(idxCursor.Current.Key);
                    }
                    catch (Exception ex)
                    {
                        WriteLine($"{ex.GetType()}: {ex.Message}");
                    }
                }                
            } while (idxCursor.MoveNext());

            WriteLine($"{db.Stats().nKeys} left in the database ({dbKeys} original count).");
            idxCursor.Close();
            dbCursor.Close();

            dbIdx.Close(true);
            db.Close(true);

            ReadLine();
        }
    }
}
