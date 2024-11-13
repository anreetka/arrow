using System;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Memory
{
    internal interface ILargeOwnableAllocation
    {
        bool TryAcquire(out IntPtr ptr, out long offset, out long length);
    }
}
