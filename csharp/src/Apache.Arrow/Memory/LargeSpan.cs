using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Apache.Arrow.Memory
{
    internal readonly struct LargeSpan<T>
    {
        private readonly LargeMemory<T> _memory;

        public LargeSpan(T[] array)
        {
            if(array == null)
            {
                _memory = LargeMemory<T>.Empty;
                return;
            }

            _memory = new LargeMemory<T>(array);
        }

        public LargeSpan(T[] array, long start, long length)
        {
            if(array == null)
            {
                if (start != 0 || length!= 0)
                {
                    throw new ArgumentOutOfRangeException();
                }

                _memory = LargeMemory<T>.Empty;
                return;
            }

            if(start + length> array.Length)
            {
                throw new ArgumentOutOfRangeException();
            }

            _memory = new LargeMemory<T>(array , start, length);
        }

        public unsafe LargeSpan(void* pointer, long length)
        {
           //implement here
        }

        public LargeSpan(ref T reference)
        {
            _memory = new LargeMemory<T>(new T[] {reference});
        }

        internal LargeSpan(ref T reference, long length)
        {
            _memory = new LargeMemory<T>(new T[] { reference }, 0, length);
        }

        public ref T this[long index]
        {
            get
            {
                if(index >= _memory.Length)
                {
                    throw new ArgumentOutOfRangeException();
                }

                //implement here
            }
        }

        public long Length => _memory.Length;

        public bool IsEmpty() => _memory.Length == 0;

        public LargeSpan<T> Slice(long start)
        {
            if(start >= _memory.Length)
            {
                throw new ArgumentOutOfRangeException();
            }

            return new LargeSpan<T>(_memory.Slice(start));
        }

        internal LargeSpan(LargeMemory<T> memory)
        {
            _memory = memory;
        }

    }
}
