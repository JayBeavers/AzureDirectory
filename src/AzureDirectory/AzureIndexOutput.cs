using System;
using System.Globalization;
using System.IO;
using System.Diagnostics;
using System.Threading;
using Microsoft.WindowsAzure.Storage.Blob;
using org.apache.lucene.store;
using Directory = org.apache.lucene.store.Directory;


namespace Lucene.Net.Store.Azure
{
    /// <summary>
    /// Implements IndexOutput semantics for a write/append only file
    /// </summary>
    public class AzureIndexOutput : IndexOutput
    {
        private readonly AzureDirectory _azureDirectory;
        private readonly string _name;
        private IndexOutput _indexOutput;
        private readonly Mutex _fileMutex;
        private ICloudBlob _blob;
        public Directory CacheDirectory { get { return _azureDirectory.CacheDirectory; } }

        public AzureIndexOutput(AzureDirectory azureDirectory, ICloudBlob blob)
        {
            _fileMutex = BlobMutexManager.GrabMutex(_name);
            _fileMutex.WaitOne();
            try
            {
                _azureDirectory = azureDirectory;
                _blob = blob;
                _name = blob.Uri.Segments[blob.Uri.Segments.Length - 1];

                // create the local cache one we will operate against...
                _indexOutput = CacheDirectory.createOutput(_name, IOContext.DEFAULT);
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public override void flush()
        {
            _indexOutput.flush();
        }

        public override void close()
        {
            _fileMutex.WaitOne();
            try
            {
                string fileName = _name;

                // make sure it's all written out
                _indexOutput.flush();

                long originalLength = _indexOutput.length();
                _indexOutput.close();

                Stream blobStream = new StreamInput(CacheDirectory.openInput(fileName, IOContext.DEFAULT));

                try
                {
                    // push the blobStream up to the cloud
                    _blob.UploadFromStream(blobStream);

                    // set the metadata with the original index file properties
                    _blob.Metadata["CachedLength"] = originalLength.ToString(CultureInfo.InvariantCulture);
                    _blob.SetMetadata();

                    Debug.WriteLine("PUT {1} bytes to {0} in cloud", _name, blobStream.Length);
                }
                finally
                {
                    blobStream.Dispose();
                }

                // clean up
                _indexOutput = null;
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
            return _indexOutput.length();
        }

        public override void writeByte(byte b)
        {
            _indexOutput.writeByte(b);
        }

        public override void writeBytes(byte[] b, int length)
        {
            _indexOutput.writeBytes(b, length);
        }

        public override void writeBytes(byte[] b, int offset, int length)
        {
            _indexOutput.writeBytes(b, offset, length);
        }

        public override long getFilePointer()
        {
            return _indexOutput.getFilePointer();
        }

        [Obsolete]
        public override void seek(long pos)
        {
            _indexOutput.seek(pos);
        }
    }
}