using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Apache.Arrow.Memory
{
    internal class LargeSpan<T>:IDisposable
    {
        private readonly LargeMemory<T> _memory;
        private long _offset;
        private long _length;

        public LargeSpan(LargeMemory<T> memory, long offset, long length)
        {
            if(memory == null) throw new ArgumentNullException(nameof(memory), "Memory cannot be null.");
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), "Start index cannot be negative.");
            if(length < 0) throw new ArgumentOutOfRangeException(nameof(length), "Length cannot be negative.");

            if( offset + length > _memory.Size)
            {
                throw new ArgumentOutOfRangeException(nameof(length), "The requested span exceeds the available memory size.");
            }

            _memory = memory;
            _offset = offset;
            _length = length;
        }

        public long Length { get { return _length; } }

        public T this[long index]
        {
            get
            {
                if (index < 0 || index >= _length)
                {
                    throw new IndexOutOfRangeException("Index is out of range within the span.");
                }

                return _memory[_offset + index];
            }

            set
            {
                if (index < 0 || index >= _length)
                {
                    throw new IndexOutOfRangeException("Index is out of range within the span.");
                }

                _memory[_offset + index] = value;
            }
        }

        public void Dispose()
        {
         
        }
    }
}
