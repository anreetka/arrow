using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace Apache.Arrow.Memory
{
    internal class NativeLargeMemoryManager: LargeMemoryManager<byte>
    {
        private IntPtr _ptr;
        private readonly long _offset;
        private readonly long _length;
        private readonly INativeAllocationOwner _owner;

        public NativeLargeMemoryManager(IntPtr ptr, long offset, long length)
            : this(NativeMemoryAllocator.ExclusiveOwner, ptr, offset, length)
        {
        }

        internal NativeLargeMemoryManager(INativeAllocationOwner owner, IntPtr ptr, long offset, long length)
        {
            _ptr = ptr;
            _offset = offset;
            _length = length;
            _owner = owner;
        }

#pragma warning disable CA2015 // TODO: is this correct?
        ~NativeLargeMemoryManager()
        {
            Dispose(false);
        }
#pragma warning restore CA2015

        public override unsafe LargeSpan<byte> GetSpan()
        {
            void* ptr = CalculatePointer(0);
            return new LargeSpan<byte>(ptr, _length);
        }

        public override unsafe MemoryHandle Pin(long elementIndex = 0)
        {
            // NOTE: Unmanaged memory doesn't require GC pinning because by definition it's not
            // managed by the garbage collector.

            void* ptr = CalculatePointer(elementIndex);
            return new MemoryHandle(ptr, default, (IPinnable)this);
        }

        public override void Unpin()
        {
            // SEE: Pin implementation
            return;
        }

        protected override void Dispose(bool disposing)
        {
            // Only free once.
            IntPtr ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
            if (ptr != IntPtr.Zero)
            {
                _owner.Release(ptr, _offset, _length);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void* CalculatePointer(long index) =>
            (void*)(_ptr + _offset + index);
    }
}
