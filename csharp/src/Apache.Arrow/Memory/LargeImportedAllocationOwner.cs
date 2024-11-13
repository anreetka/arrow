using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Apache.Arrow.Memory
{
    internal abstract class LargeImportedAllocationOwner:ILargeNativeAllocationOwner
    {

        private long _referenceCount;
        private long _managedMemory;

        protected LargeImportedAllocationOwner()
        {
            _referenceCount = 1;
        }

        public ILargeMemoryOwner<byte> AddMemory(IntPtr ptr, long offset, long length)
        {
            if (_referenceCount <= 0)
            {
                throw new ObjectDisposedException(typeof(LargeImportedAllocationOwner).Name);
            }

            LargeNativeMemoryManager memory = new LargeNativeMemoryManager(this, ptr, offset, length);
            Interlocked.Increment(ref _referenceCount);

            if (length > 0)
            {
                Interlocked.Add(ref _managedMemory, length);
                GC.AddMemoryPressure(length);
            }

            return memory;
        }

        public void Release(IntPtr ptr, long offset, long length)
        {
            Release();
        }

        public void Release()
        {
            if (Interlocked.Decrement(ref _referenceCount) == 0)
            {
                if (_managedMemory > 0)
                {
                    GC.RemoveMemoryPressure(_managedMemory);
                }
                FinalRelease();
            }
        }

         protected abstract void FinalRelease();
    }
}
