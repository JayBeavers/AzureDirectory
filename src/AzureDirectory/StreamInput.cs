using org.apache.lucene.store;
using System;
using System.IO;

namespace Lucene.Net.Store.Azure
{
    /// <summary>
    /// Stream wrapper around IndexInput
    /// </summary>
    public class StreamInput : Stream
    {
        public IndexInput Input { get; set; }

        public StreamInput(IndexInput input)
        {
            Input = input;
        }

        public override bool CanRead { get { return true; } }
        public override bool CanSeek { get { return true; } }
        public override bool CanWrite { get { return false; } }
        public override void Flush() { }
        public override long Length { get { return Input.length(); } }

        public override long Position
        {
            get { return Input.getFilePointer(); }
            set { Input.seek(value); }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            long pos = Input.getFilePointer();
            try
            {
                long len = Input.length();
                if (count > (len - pos))
                    count = (int) (len - pos);
                Input.readBytes(buffer, offset, count);
            }
            catch
            {    
            }

            return (int)(Input.getFilePointer() - pos);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    Input.seek(offset);
                    break;
                case SeekOrigin.Current:
                    Input.seek(Input.getFilePointer() + offset);
                    break;
                case SeekOrigin.End:
                    throw new NotImplementedException();
            }
            return Input.getFilePointer();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override void Close()
        {
            Input.close();
            base.Close();
        }
    }
}