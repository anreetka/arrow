using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.InteropServices.Marshalling;
using System.Text;

namespace Apache.Arrow.Memory
{
    public abstract class LargeMemoryManager<T>:ILargeMemoryOwner<T>, IPinnable
    {
        public virtual LargeMemory<T> LargeMemory => new LargeMemory<T>(this, GetSpan().Length);

        public abstract Span<T> GetSpan();

        public abstract MemoryHandle Pin(long elementIndex = 0);

        public abstract void Unpin();

        protected LargeMemory<T> CreateMemory(long length) => new LargeMemory<T>(this, length);

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
