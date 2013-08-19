using System;
using System.Diagnostics;
using System.Threading;
using Microsoft.WindowsAzure.Storage.Blob;
using org.apache.lucene.store;

namespace Lucene.Net.Store.Azure
{
    /// <summary>
    /// Implements IndexInput semantics for a read only blob
    /// </summary>
    public class AzureIndexInput : IndexInput
    {
        private AzureDirectory _azureDirectory;
        private CloudBlobContainer _blobContainer;
        private ICloudBlob _blob;
        private readonly string _name;

        private IndexInput _indexInput;
        private readonly Mutex _fileMutex;

        public Directory CacheDirectory { get { return _azureDirectory.CacheDirectory; } }

        public AzureIndexInput(AzureDirectory azuredirectory, ICloudBlob blob) : base(blob.Name)
        {
            _name = blob.Uri.Segments[blob.Uri.Segments.Length - 1];

            _fileMutex = BlobMutexManager.GrabMutex(_name);
            _fileMutex.WaitOne();

            try
            {
                _azureDirectory = azuredirectory;
                _blobContainer = azuredirectory.BlobContainer;
                _blob = blob;

                string fileName = _name;
                StreamOutput fileStream = _azureDirectory.CreateCachedOutputAsStream(fileName);

                // get the blob
                _blob.DownloadToStream(fileStream);

                fileStream.Flush();
                Debug.WriteLine("GET {0} RETREIVED {1} bytes", _name, fileStream.Length);

                fileStream.Close();

                // and open it as an input 
                _indexInput = CacheDirectory.openInput(fileName, IOContext.DEFAULT);
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public AzureIndexInput(AzureIndexInput cloneInput) : base(cloneInput._blob.Name)
        {
            _fileMutex = BlobMutexManager.GrabMutex(cloneInput._name);
            _fileMutex.WaitOne();

            try
            {
                _azureDirectory = cloneInput._azureDirectory;
                _blobContainer = cloneInput._blobContainer;
                _blob = cloneInput._blob;
                _indexInput = cloneInput._indexInput.clone();
            }
            catch (Exception)
            {
                // sometimes we get access denied on the 2nd stream...but not always. I haven't tracked it down yet
                // but this covers our tail until I do
                Debug.WriteLine(String.Format("Dagnabbit, falling back to memory clone for {0}", cloneInput._name));
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public override byte readByte()
        {
            return _indexInput.readByte();
        }

        public override void readBytes(byte[] b, int offset, int len)
        {
            _indexInput.readBytes(b, offset, len);
        }

        public override long getFilePointer()
        {
            return _indexInput.getFilePointer();
        }

        public override void seek(long pos)
        {
            _indexInput.seek(pos);
        }

        public override void close()
        {
            _fileMutex.WaitOne();
            try
            {
                _indexInput.close();
                _indexInput = null;
                _azureDirectory = null;
                _blobContainer = null;
                _blob = null;
                GC.SuppressFinalize(this);
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public override long length()
        {
            return _indexInput.length();
        }

        public override IndexInput clone()
        {
            IndexInput clone = null;
            try
            {
                _fileMutex.WaitOne();
                var input = new AzureIndexInput(this);
                clone = input;
            }
            catch (Exception err)
            {
                Debug.WriteLine(err.ToString());
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
            Debug.Assert(clone != null);
            return clone;
        }
    }
}