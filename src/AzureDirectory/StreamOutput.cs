using org.apache.lucene.store;
using System;
using System.IO;

namespace Lucene.Net.Store.Azure
{
    /// <summary>
    /// Stream wrapper around an IndexOutput
    /// </summary>
    public class StreamOutput : Stream
    {
        public IndexOutput Output { get; set; }

        public StreamOutput(IndexOutput output)
        {
            Output = output;
        }

        public override bool CanRead
        {
            get { return false; }
        }

        public override bool CanSeek
        {
            get { return true; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override void Flush()
        {
            Output.flush();
        }

        public override long Length
        {
            get { return Output.length(); }
        }

        public override long Position
        {
            get
            {
                return Output.getFilePointer();
            }
            set
            {
                Output.seek(value);
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    Output.seek(offset);
                    break;
                case SeekOrigin.Current:
                    Output.seek(Output.getFilePointer() + offset);
                    break;
                case SeekOrigin.End:
                    throw new System.NotImplementedException();
            }
            return Output.getFilePointer();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            Output.writeBytes(buffer, offset, count);
        }

        public override void Close()
        {
            Output.flush();
            Output.close();
            base.Close();
        }
    }
}

