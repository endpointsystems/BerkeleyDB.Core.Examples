namespace GettingStarted.DataWriting
{

    public interface IVendorRepository
    {
        void AddVendor(Vendor v);
        void Save();
    }

    public interface IInventoryRepository
    {
        void AddInventory(string sku, Inventory inv);
        void Save();
    }

    public interface ICsvReader
    {
        void UploadData();
    }

}
