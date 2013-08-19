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
        private readonly string _catalog;
        private CloudBlobClient _blobClient;
        private Directory _cacheDirectory;

        /// <summary>
        /// Create an AzureDirectory
        /// </summary>
        /// <param name="storageAccount">storage account to use</param>
        /// <param name="catalog">name of catalog (folder in blob storage)</param>
        /// <param name="cacheDirectory">local Directory object to use for local cache</param>
        public AzureDirectory(CloudStorageAccount storageAccount, string catalog = null, Directory cacheDirectory = null)
        {
            if (storageAccount == null)
                throw new ArgumentNullException("storageAccount");

            _catalog = string.IsNullOrEmpty(catalog) ? "lucene" : catalog.ToLower();

            _blobClient = storageAccount.CreateCloudBlobClient();
            _initCacheDirectory(cacheDirectory);
        }

        public CloudBlobContainer BlobContainer { get; private set; }

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

        private void _initCacheDirectory(Directory cacheDirectory)
        {
            if (cacheDirectory != null)
            {
                // save it off
                _cacheDirectory = cacheDirectory;
            }
            else
            {
                string cachePath = System.IO.Path.Combine(Environment.ExpandEnvironmentVariables("%temp%"), "AzureDirectory");
                var azureDir = new System.IO.DirectoryInfo(cachePath);
                if (!azureDir.Exists)
                    azureDir.Create();

                string catalogPath = System.IO.Path.Combine(cachePath, _catalog);
                var catalogDir = new System.IO.DirectoryInfo(catalogPath);
                if (!catalogDir.Exists)
                    catalogDir.Create();

                _cacheDirectory = FSDirectory.open(new File(catalogPath));
            }

            CreateContainer();
        }

        public void CreateContainer()
        {
            BlobContainer = _blobClient.GetContainerReference(_catalog);

            // create it if it does not exist
            BlobContainer.CreateIfNotExists();
        }

        /// <summary>Returns an array of strings, one for each file in the directory. </summary>
        public override String[] listAll()
        {
            var results = from blob in BlobContainer.ListBlobs()
                          select blob.Uri.AbsolutePath.Substring(blob.Uri.AbsolutePath.LastIndexOf('/') + 1);
            return results.ToArray();
        }

        /// <summary>Returns true if a file with the given name exists. </summary>
        public override bool fileExists(String name)
        {
            // this always comes from the server
            try
            {
                var blob = BlobContainer.GetBlockBlobReference(name);
                blob.FetchAttributes();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }
        
        /// <summary>Removes an existing file in the directory. </summary>
        public override void deleteFile(String name)
        {
            var blob = BlobContainer.GetBlockBlobReference(name);
            blob.DeleteIfExists();
            Debug.WriteLine("DELETE {0}/{1}", BlobContainer.Uri, name);

            if (_cacheDirectory.fileExists(name + ".blob"))
                _cacheDirectory.deleteFile(name + ".blob");

            if (_cacheDirectory.fileExists(name))
                _cacheDirectory.deleteFile(name);
        }

        /// <summary>Returns the length of a file in the directory. </summary>
        public override long fileLength(String name)
        {
            var blob = BlobContainer.GetBlockBlobReference(name);
            blob.FetchAttributes();

            // index files may be compressed so the actual length is stored in metatdata
            long blobLength;
            if (long.TryParse(blob.Metadata["CachedLength"], out blobLength))
            {
                return blobLength;
            }

            return blob.Properties.Length; // fall back to actual blob size
        }

        /// <summary>Creates a new, empty file in the directory with the given name.
        /// Returns a stream writing this file. 
        /// </summary>
        public override IndexOutput createOutput(string name, IOContext ioc)
        {
            var blob = BlobContainer.GetBlockBlobReference(name);
            return new AzureIndexOutput(this, blob);
        }

        /// <summary>Returns a stream reading an existing file. </summary>
        public override IndexInput openInput(string name, IOContext ioc)
        {
            try
            {
                var blob = BlobContainer.GetBlockBlobReference(name);
                blob.FetchAttributes();
                var input = new AzureIndexInput(this, blob);
                return input;
            }
            catch (StorageException)
            {
                throw new NoSuchFileException(name);
            }
        }

        private readonly Dictionary<string, AzureLock> _locks = new Dictionary<string, AzureLock>();

        /// <summary>Construct a {@link Lock}.</summary>
        /// <param name="name">the name of the lock file
        /// </param>
        public override Lock makeLock(String name)
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
            BlobContainer = null;
            _blobClient = null;
        }

        public StreamInput OpenCachedInputAsStream(string name)
        {
            return new StreamInput(CacheDirectory.openInput(name, IOContext.DEFAULT));
        }

        public StreamOutput CreateCachedOutputAsStream(string name)
        {
            return new StreamOutput(CacheDirectory.createOutput(name, IOContext.DEFAULT));
        }

        public override void sync(java.util.Collection c)
        {
        }
    }
}