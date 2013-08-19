using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using org.apache.lucene.store;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace Lucene.Net.Store.Azure
{
    /// <summary>
    /// Implements lock semantics on AzureDirectory via a blob lease
    /// </summary>
    public class AzureLock : Lock
    {
        private readonly string _lockFile;
        private readonly AzureDirectory _azureDirectory;
        private string _leaseid;

        public AzureLock(string lockFile, AzureDirectory directory)
        {
            _lockFile = lockFile;
            _azureDirectory = directory;
        }

        override public bool isLocked()
        {
            var blob = _azureDirectory.BlobContainer.GetBlobReferenceFromServer(_lockFile);
            try
            {
                Debug.Print("IsLockeD() : {0}", _leaseid);
                if (String.IsNullOrEmpty(_leaseid))
                {
                    var tempLease = blob.AcquireLease(TimeSpan.FromSeconds(60), _leaseid);
                    if (String.IsNullOrEmpty(tempLease))
                    {
                        Debug.Print("IsLocked() : TRUE");
                        return true;
                    }
                    blob.ReleaseLease(new AccessCondition { LeaseId = tempLease });
                }
                Debug.Print("IsLocked() : {0}", _leaseid);
                return String.IsNullOrEmpty(_leaseid);
            }
            catch (StorageException webErr)
            {
                if (_handleWebException(blob, webErr))
                    return isLocked();
            }
            /*catch (StorageClientException err)
            {
                if (_handleStorageClientException(blob, err))
                    return IsLocked();
            }*/
            _leaseid = null;
            return false;
        }

        public override bool obtain()
        {
            var blob = _azureDirectory.BlobContainer.GetBlockBlobReference(_lockFile);
            try
            {
                Debug.Print("AzureLock:Obtain({0}) : {1}", _lockFile, _leaseid);
                if (String.IsNullOrEmpty(_leaseid))
                {
                    _leaseid = blob.AcquireLease(TimeSpan.FromSeconds(60), _leaseid);
                    Debug.Print("AzureLock:Obtain({0}): AcquireLease : {1}", _lockFile, _leaseid);

                    // keep the lease alive by renewing every 30 seconds
                    var interval = (long)TimeSpan.FromSeconds(30).TotalMilliseconds;
                    _renewTimer = new Timer(obj =>
                    {
                        try
                        {
                            var al = (AzureLock)obj;
                            al.Renew();
                        }
                        catch (Exception err) { Debug.Print(err.ToString()); }
                    }, this, interval, interval);
                }
                return !String.IsNullOrEmpty(_leaseid);
            }
            catch (StorageException webErr)
            {
                if (_handleWebException(blob, webErr))
                    return obtain();
            }

            return false;
        }

        private Timer _renewTimer;

        public void Renew()
        {
            if (!String.IsNullOrEmpty(_leaseid))
            {
                Debug.Print("AzureLock:Renew({0} : {1}", _lockFile, _leaseid);
                var blob = _azureDirectory.BlobContainer.GetBlockBlobReference(_lockFile);
                blob.RenewLease(new AccessCondition { LeaseId = _leaseid });
            }
        }

        public override void release()
        {
            Debug.Print("AzureLock:Release({0}) {1}", _lockFile, _leaseid);
            if (!String.IsNullOrEmpty(_leaseid))
            {
                var blob = _azureDirectory.BlobContainer.GetBlockBlobReference(_lockFile);
                blob.ReleaseLease(new AccessCondition { LeaseId = _leaseid });
                if (_renewTimer != null)
                {
                    _renewTimer.Dispose();
                    _renewTimer = null;
                }
                _leaseid = null;
            }
        }

        public void BreakLock()
        {
            Debug.Print("AzureLock:BreakLock({0}) {1}", _lockFile, _leaseid);
            var blob = _azureDirectory.BlobContainer.GetBlockBlobReference(_lockFile);
            try
            {
                blob.BreakLease();
            }
            catch
            {
            }

            _leaseid = null;
        }

        public new String ToString()
        {
            return String.Format("AzureLock@{0}.{1}", _lockFile, _leaseid);
        }

        private bool _handleWebException(ICloudBlob blob, StorageException err)
        {
            if (err.RequestInformation.HttpStatusCode == 404)
            {
                _azureDirectory.CreateContainer();
                using (var stream = new MemoryStream())
                using (var writer = new StreamWriter(stream))
                {
                    writer.Write(_lockFile);
                    blob.UploadFromStream(stream);
                }
                return true;
            }
            return false;
        }
    }
}