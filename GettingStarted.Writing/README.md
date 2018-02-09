# About This Project

This first project demonstrates taking 50k vendors, each with 5 inventory items, and inserting them one by one into a vendor and inventory database, respectively. This is Berkeley DB at its most basic - no transactions, no environments, just a simple `Database.Put` into a BTree database. A couple of things to note:

- We're taking our data objects, serializing them, and then Base64 encoding them. We could save them in a number of different formats - serialized JSON or XML, for example - but for now we're just trying to write all the data into the database as quickly as possible. 
- Speaking of quickly - the last run performed on a Surface 4 Pro machine with this code, loading all vendor and inventory data from the CSV files, took 12.608 seconds, averaging 501.13806 ticks to write 1 vendor and 5 inventory records (note there are 10,000 ticks in a millisecond). The fastest write time was 216 ticks, and the slowest was 210794 (~21 milliseconds).
- We explicitly call the `Sync` function in order to save our data to the database. 

