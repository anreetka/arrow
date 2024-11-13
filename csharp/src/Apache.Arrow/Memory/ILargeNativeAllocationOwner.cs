using System;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Memory
{
    internal interface ILargeNativeAllocationOwner
    {
        void Release(IntPtr ptr, long offset, long length);
    }
}
