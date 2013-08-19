using java.io;
using java.nio.file;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using org.apache.lucene.store;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Lucene.Net.Store.Azure
{
    public class AzureDirectory : Directory
    {
        private string _catalog;
        private CloudBlobClient _blobClient;
        private CloudBlobContainer _blobContainer;
        private Directory _cacheDirectory;

        #region CTOR
        public AzureDirectory(CloudStorageAccount storageAccount) :
            this(storageAccount, null, null)
        {
        }

        /// <summary>
        /// Create AzureDirectory
        /// </summary>
        /// <param name="storageAccount">staorage account to use</param>
        /// <param name="catalog">name of catalog (folder in blob storage)</param>
        /// <remarks>Default local cache is to use file system in user/appdata/AzureDirectory/Catalog</remarks>
        public AzureDirectory(
            CloudStorageAccount storageAccount,
            string catalog)
            : this(storageAccount, catalog, null)
        {
        }

        /// <summary>
        /// Create an AzureDirectory
        /// </summary>
        /// <param name="storageAccount">storage account to use</param>
        /// <param name="catalog">name of catalog (folder in blob storage)</param>
        /// <param name="cacheDirectory">local Directory object to use for local cache</param>
        public AzureDirectory(
            CloudStorageAccount storageAccount,
            string catalog,
            Directory cacheDirectory)
        {
            if (storageAccount == null)
                throw new ArgumentNullException("storageAccount");

            if (string.IsNullOrEmpty(catalog))
                _catalog = "lucene";
            else
                _catalog = catalog.ToLower();

            _blobClient = storageAccount.CreateCloudBlobClient();
            _initCacheDirectory(cacheDirectory);
        }

        public CloudBlobContainer BlobContainer
        {
            get
            {
                return _blobContainer;
            }
        }

#if COMPRESSBLOBS
        public bool CompressBlobs
        {
            get;
            set;
        }
#endif
        public void ClearCache()
        {
            foreach (string file in _cacheDirectory.listAll())
            {
                _cacheDirectory.deleteFile(file);
            }
        }

        public Directory CacheDirectory
        {
            get
            {
                return _cacheDirectory;
            }
            set
            {
                _cacheDirectory = value;
            }
        }
        #endregion

        #region internal methods
        private void _initCacheDirectory(Directory cacheDirectory)
        {
#if COMPRESSBLOBS
            CompressBlobs = true;
#endif
            if (cacheDirectory != null)
            {
                // save it off
                _cacheDirectory = cacheDirectory;
            }
            else
            {
                string cachePath = System.IO.Path.Combine(Environment.ExpandEnvironmentVariables("%temp%"), "AzureDirectory");
                System.IO.DirectoryInfo azureDir = new System.IO.DirectoryInfo(cachePath);
                if (!azureDir.Exists)
                    azureDir.Create();

                string catalogPath = System.IO.Path.Combine(cachePath, _catalog);
                System.IO.DirectoryInfo catalogDir = new System.IO.DirectoryInfo(catalogPath);
                if (!catalogDir.Exists)
                    catalogDir.Create();

                _cacheDirectory = FSDirectory.open(new File(catalogPath));
            }

            CreateContainer();
        }

        public void CreateContainer()
        {
            _blobContainer = _blobClient.GetContainerReference(_catalog);

            // create it if it does not exist
            _blobContainer.CreateIfNotExists();
        }
        #endregion

        #region DIRECTORY METHODS
        /// <summary>Returns an array of strings, one for each file in the directory. </summary>
        public override System.String[] listAll()
        {
            var results = from blob in _blobContainer.ListBlobs()
                          select blob.Uri.AbsolutePath.Substring(blob.Uri.AbsolutePath.LastIndexOf('/') + 1);
            return results.ToArray<string>();
        }

        /// <summary>Returns true if a file with the given name exists. </summary>
        public override bool fileExists(System.String name)
        {
            // this always comes from the server
            try
            {
                var blob = _blobContainer.GetBlockBlobReference(name);
                blob.FetchAttributes();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }
        
        /// <summary>Removes an existing file in the directory. </summary>
        public override void deleteFile(System.String name)
        {
            var blob = _blobContainer.GetBlockBlobReference(name);
            blob.DeleteIfExists();
            Debug.WriteLine(String.Format("DELETE {0}/{1}", _blobContainer.Uri.ToString(), name));

            if (_cacheDirectory.fileExists(name + ".blob"))
                _cacheDirectory.deleteFile(name + ".blob");

            if (_cacheDirectory.fileExists(name))
                _cacheDirectory.deleteFile(name);
        }

        /*
        /// <summary>Renames an existing file in the directory.
        /// If a file already exists with the new name, then it is replaced.
        /// This replacement should be atomic. 
        /// </summary>
        public void RenameFile(System.String from, System.String to)
        {
            try
            {
                var blobFrom = _blobContainer.GetBlockBlobReference(from);
                var blobTo = _blobContainer.GetBlockBlobReference(to);
                blobTo.CopyFromBlob(blobFrom);
                blobFrom.DeleteIfExists();

                // we delete and force a redownload, since we can't do this in an atomic way
                if (_cacheDirectory.FileExists(from))
                    _cacheDirectory.RenameFile(from, to);

                // drop old cached data as it's wrong now
                if (_cacheDirectory.FileExists(from + ".blob"))
                    _cacheDirectory.DeleteFile(from + ".blob");
            }
            catch
            {
            }
        }*/

        /// <summary>Returns the length of a file in the directory. </summary>
        public override long fileLength(System.String name)
        {
            var blob = _blobContainer.GetBlockBlobReference(name);
            blob.FetchAttributes();

            // index files may be compressed so the actual length is stored in metatdata
            long blobLength;
            if (long.TryParse(blob.Metadata["CachedLength"], out blobLength))
                return blobLength;
            else
                return blob.Properties.Length; // fall back to actual blob size
        }

        /// <summary>Creates a new, empty file in the directory with the given name.
        /// Returns a stream writing this file. 
        /// </summary>
        public override IndexOutput createOutput(string name, IOContext ioc)
        {
            var blob = _blobContainer.GetBlockBlobReference(name);
            return new AzureIndexOutput(this, blob);
        }

        /// <summary>Returns a stream reading an existing file. </summary>
        public override IndexInput openInput(string name, IOContext ioc)
        {
            try
            {
                var blob = _blobContainer.GetBlockBlobReference(name);
                blob.FetchAttributes();
                AzureIndexInput input = new AzureIndexInput(this, blob);
                return input;
            }
            catch (StorageException err)
            {
                throw new NoSuchFileException(name);
            }
        }

        private Dictionary<string, AzureLock> _locks = new Dictionary<string, AzureLock>();

        /// <summary>Construct a {@link Lock}.</summary>
        /// <param name="name">the name of the lock file
        /// </param>
        public override Lock makeLock(System.String name)
        {
            lock (_locks)
            {
                if (!_locks.ContainsKey(name))
                    _locks.Add(name, new AzureLock(name, this));
                return _locks[name];
            }
        }

        public override void clearLock(string name)
        {
            lock (_locks)
            {
                if (_locks.ContainsKey(name))
                {
                    _locks[name].BreakLock();
                }
            }
            _cacheDirectory.clearLock(name);
        }

        /// <summary>Closes the store. </summary>
        public override void close()
        {
            _blobContainer = null;
            _blobClient = null;
        }
        #endregion

        #region Azure specific methods
#if COMPRESSBLOBS
        public virtual bool ShouldCompressFile(string path)
        {
            if (!CompressBlobs)
                return false;

            string ext = System.IO.Path.GetExtension(path);
            switch (ext)
            {
                case ".cfs":
                case ".fdt":
                case ".fdx":
                case ".frq":
                case ".tis":
                case ".tii":
                case ".nrm":
                case ".tvx":
                case ".tvd":
                case ".tvf":
                case ".prx":
                    return true;
                default:
                    return false;
            };
        }
#endif
        public StreamInput OpenCachedInputAsStream(string name)
        {
            return new StreamInput(CacheDirectory.openInput(name, IOContext.DEFAULT));
        }

        public StreamOutput CreateCachedOutputAsStream(string name)
        {
            return new StreamOutput(CacheDirectory.createOutput(name, IOContext.DEFAULT));
        }

        #endregion

        public override void sync(java.util.Collection c)
        {
        }
    }
}