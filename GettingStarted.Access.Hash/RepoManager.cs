using System;
using System.Collections.Generic;
using System.Text;
using BerkeleyDB.Core;

namespace GettingStarted.Access.Hash
{
    /// <summary>
    /// Manages the <see cref="BerkeleyDB.Core.DatabaseEnvironment"/> and database repositories associated with it.
    /// </summary>
    public class RepoManager
    {
        private DatabaseEnvironment env;

        public VendorRepository VendorRepository { get; set; }
        public InventoryRepository InventoryRepository { get; set; }

        public RepoManager(string dataPath)
        {
            DatabaseEnvironment env;
            DatabaseEnvironmentConfig cfg = new DatabaseEnvironmentConfig
            {
                MPoolSystemCfg = new MPoolConfig {CacheSize = new CacheInfo(1, 0, 1)},
                Create = true,
                CreationDir = dataPath,
                ErrorPrefix = "hash",

            };
            env = DatabaseEnvironment.Open(dataPath, cfg);
            VendorRepository = new VendorRepository(dataPath);
        }
    }
}
