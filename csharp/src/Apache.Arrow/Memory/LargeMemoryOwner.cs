using System;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Memory
{
    internal class LargeMemoryOwner<T>:IDisposable
    {
        private readonly LargeMemory<T> _memory;

        public LargeMemoryOwner(long size)
        {
            _memory = new LargeMemory<T>(size);
        }

        public LargeMemory<T> Memory {  get { return _memory; } }

        public void Dispose()
        {
            _memory.Dispose();
        }
    }
}
