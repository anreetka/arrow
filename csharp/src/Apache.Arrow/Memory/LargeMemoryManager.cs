using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Apache.Arrow.Memory
{
    public abstract class LargeMemoryManager<T>:ILargeMemoryOwner<T>
    {
        public virtual LargeMemory<T> LargeMemory => new LargeMemory<T>(this, GetSpan().Length);

        public abstract LargeSpan<T> GetSpan();

        public abstract MemoryHandle Pin(long elementIndex = 0);

        public abstract void Unpin();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected LargeMemory<T> CreateMemory(long length) => new LargeMemory<T>(this, length);


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected LargeMemory<T> CreateMemory(long start, long length) => new LargeMemory<T>(this, start, length);

        protected internal virtual bool TryGetArray(out ArraySegment<T> segment)
        {
            segment = default;
            return false;
        }

        void IDisposable.Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        protected abstract void Dispose(bool disposing);

    }
}
