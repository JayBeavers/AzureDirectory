//    License: Microsoft Public License (Ms-PL) 
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
//using System.IO;
using System.Diagnostics;
using System.IO.Compression;
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
        private string _name;

        private IndexInput _indexInput;
        private Mutex _fileMutex;

        public Directory CacheDirectory { get { return _azureDirectory.CacheDirectory; } }

        public AzureIndexInput(AzureDirectory azuredirectory, ICloudBlob blob) : base(blob.Name)
        {
            _name = blob.Uri.Segments[blob.Uri.Segments.Length - 1];

#if FULLDEBUG
            Debug.WriteLine(String.Format("opening {0} ", _name));
#endif
            _fileMutex = BlobMutexManager.GrabMutex(_name);
            _fileMutex.WaitOne();
            try
            {
                _azureDirectory = azuredirectory;
                _blobContainer = azuredirectory.BlobContainer;
                _blob = blob;

                string fileName = _name;

#if COMPRESSBLOBS
                if (_azureDirectory.ShouldCompressFile(_name))
                {
                    // then we will get it fresh into local deflatedName 
                    // StreamOutput deflatedStream = new StreamOutput(CacheDirectory.CreateOutput(deflatedName));
                    var deflatedStream = new System.IO.MemoryStream();

                    // get the deflated blob
                    _blob.DownloadToStream(deflatedStream);

                    Debug.WriteLine(string.Format("GET {0} RETREIVED {1} bytes", _name, deflatedStream.Length));

                    // seek back to begininng
                    deflatedStream.Seek(0, System.IO.SeekOrigin.Begin);

                    // open output file for uncompressed contents
                    StreamOutput fileStream = _azureDirectory.CreateCachedOutputAsStream(fileName);

                    // create decompressor
                    DeflateStream decompressor = new DeflateStream(deflatedStream, CompressionMode.Decompress);

                    byte[] bytes = new byte[65535];
                    int nRead = 0;
                    do
                    {
                        nRead = decompressor.Read(bytes, 0, 65535);
                        if (nRead > 0)
                            fileStream.Write(bytes, 0, nRead);
                    } while (nRead == 65535);
                    decompressor.Close(); // this should close the deflatedFileStream too

                    fileStream.Close();

                }
                else
#endif
                {
                    StreamOutput fileStream = _azureDirectory.CreateCachedOutputAsStream(fileName);

                    // get the blob
                    _blob.DownloadToStream(fileStream);

                    fileStream.Flush();
                    Debug.WriteLine(string.Format("GET {0} RETREIVED {1} bytes", _name, fileStream.Length));

                    fileStream.Close();
                }

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
#if FULLDEBUG
                Debug.WriteLine(String.Format("Creating clone for {0}", cloneInput._name));
#endif
                _azureDirectory = cloneInput._azureDirectory;
                _blobContainer = cloneInput._blobContainer;
                _blob = cloneInput._blob;
                _indexInput = cloneInput._indexInput.clone() as IndexInput;
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
#if FULLDEBUG
                Debug.WriteLine(String.Format("CLOSED READSTREAM local {0}", _name));
#endif
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
                AzureIndexInput input = new AzureIndexInput(this);
                clone = (IndexInput)input;
            }
            catch (System.Exception err)
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

