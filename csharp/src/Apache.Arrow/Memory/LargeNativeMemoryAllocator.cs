using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace Apache.Arrow.Memory
{
    internal class LargeNativeMemoryAllocator:LargeMemoryAllocator
    {
        internal static readonly ILargeNativeAllocationOwner ExclusiveOwner = new LargeNativeAllocationOwner();

        public LargeNativeMemoryAllocator(int alignment = DefaultAlignment)
            : base(alignment) { }

        protected override ILargeMemoryOwner<byte> AllocateInternal(long length, out long bytesAllocated)
        {
            long size = length + Alignment;
            IntPtr ptr = Marshal.AllocHGlobal(size);
            long offset = (Alignment - (ptr.ToInt64() & (Alignment - 1)));
            var manager = new LargeNativeMemoryManager(ptr, offset, length);

            bytesAllocated = (length + Alignment);

            GC.AddMemoryPressure(bytesAllocated);

            // Ensure all allocated memory is zeroed.
            manager.LargeMemory.Span.Fill(0);

            return manager;
        }

        private sealed class LargeNativeAllocationOwner : ILargeNativeAllocationOwner
        {
            public void Release(IntPtr ptr, long offset, long length)
            {
                Marshal.FreeHGlobal(ptr);
                GC.RemoveMemoryPressure(length);
            }
        }
    }
}
