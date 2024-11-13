using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace Apache.Arrow.Memory
{
    internal class LargeNativeMemoryManager: LargeMemoryManager<byte>, ILargeOwnableAllocation
    {
        private IntPtr _ptr;
        private readonly long _offset;
        private readonly long _length;
        private readonly ILargeNativeAllocationOwner _owner;

        public LargeNativeMemoryManager(IntPtr ptr, long offset, long length)
            :this(LargeNativeMemoryAllocator.ExclusiveOwner, ptr, offset, length)
        {
        }

        internal LargeNativeMemoryManager(ILargeNativeAllocationOwner owner, IntPtr ptr, long offset, long length)
        {
            _ptr = ptr;
            _offset = offset;
            _length = length;
            _owner = owner;
        }
        ~LargeNativeMemoryManager()
        {
            Dispose(false);
        }

        public override unsafe Span<byte> GetSpan()
        {
            void* ptr = CalculatePointer(0);
            return new Span<byte>(ptr, _length);
        }

        public override unsafe MemoryHandle Pin(long elementIndex = 0)
        {
            void* ptr = CalculatePointer(elementIndex);
            return new MemoryHandle(ptr, default, this);
        }

        public override void Unpin()
        {
            return;
        }

        protected override void Dispose(bool disposing)
        {
            if(_ptr != IntPtr.Zero)
            {
                IntPtr ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
                if (ptr != IntPtr.Zero)
                {
                    _owner.Release(ptr, _offset, _length);
                }
            }
        }

        bool ILargeOwnableAllocation.TryAcquire(out IntPtr ptr, out long offset, out long length)
        {
            if (object.ReferenceEquals(_owner, LargeNativeMemoryAllocator.ExclusiveOwner))
            {
                ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
                if (ptr != IntPtr.Zero)
                {
                    offset = _offset;
                    length = _length;
                    return true;
                }
            }

            ptr = IntPtr.Zero;
            offset = 0;
            length = 0;
            return false;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void* CalculatePointer(long index) =>
            (void*)(_ptr + _offset + index);
    }
}
