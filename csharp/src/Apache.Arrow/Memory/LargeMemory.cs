using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.InteropServices;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Apache.Arrow.Memory
{
    internal class LargeMemory<T>: IDisposable
    {
        private IntPtr _ptr;
        private long _size;

        public LargeMemory(long size)
        {
            if(_size < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(size), "Length cannot be negative.");
            }

            _ptr = Marshal.AllocHGlobal((IntPtr)size);
            _size = size;
        }

        public T this[long index]
        {
            get
            {
                if(index < 0 || index >= _size)
                {
                    throw new IndexOutOfRangeException();
                }

                return Marshal.PtrToStructure<T>(IntPtr.Add(_ptr, (int)(index * Marshal.SizeOf<T>())));
            }

            set
            {
                if (index < 0 || index >= _size)
                {
                    throw new IndexOutOfRangeException();
                }

                IntPtr targetAddr = IntPtr.Add(_ptr, (int)(index * Marshal.SizeOf<T>()));
                Marshal.StructureToPtr<T>(value, targetAddr, false);
                
            }
        }

        public LargeMemory<T> Slice(long start, long end)
        {
            if (start < 0 || start + end > _size)
            {
                throw new ArgumentOutOfRangeException();
            }

            IntPtr slicePtr = IntPtr.Add(_ptr, (int)(start * Marshal.SizeOf<T>()));
            return new LargeMemory<T>(end) { _ptr = slicePtr };
        }

        public long Size { get { return _size; } }

        public void Dispose()
        {
            if (_ptr != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(_ptr);
                _ptr = IntPtr.Zero;
            }
        }



    }
}
