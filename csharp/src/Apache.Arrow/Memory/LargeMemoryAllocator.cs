using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Apache.Arrow.Memory
{
    internal abstract class LargeMemoryAllocator
    {
        public const int DefaultAlignment = 64;

        private static ILargeMemoryOwner<byte> NullMemoryOwner { get; } = new LargeNullMemoryOwner();

        public static Lazy<LargeMemoryAllocator> Default { get; } = new Lazy<LargeMemoryAllocator>(BuildDefault, true);

        public class Stats
        {
            private long _bytesAllocated;
            private long _allocations;

            public long Allocations => Interlocked.Read(ref _allocations);
            public long BytesAllocated => Interlocked.Read(ref _bytesAllocated);
            internal void Allocate(long n)
            {
                Interlocked.Increment(ref _allocations);
                Interlocked.Add(ref _bytesAllocated, n);
            }
        }

        public Stats Statistics { get; }

        protected int Alignment { get; }

        protected LargeMemoryAllocator(int alignment = DefaultAlignment)
        {
            Statistics = new Stats();
            Alignment = alignment;
        }

        public ILargeMemoryOwner<byte> Allocate(long length)
        {
            if (length < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            if (length == 0)
            {
                return NullMemoryOwner;
            }

            ILargeMemoryOwner<byte> memoryOwner = AllocateInternal(length, out long bytesAllocated);

            Statistics.Allocate(bytesAllocated);

            return memoryOwner;
        }

        private static LargeMemoryAllocator BuildDefault()
        {
            return new LargeNativeMemoryAllocator(DefaultAlignment);
        }

        protected abstract ILargeMemoryOwner<byte> AllocateInternal(long length, out long bytesAllocated);
    }
}
