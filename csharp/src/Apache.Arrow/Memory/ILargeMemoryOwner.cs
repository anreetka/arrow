using System;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Memory
{
    public interface ILargeMemoryOwner<T>:IDisposable
    {
        LargeMemory<T> LargeMemory { get; }
    }
}
