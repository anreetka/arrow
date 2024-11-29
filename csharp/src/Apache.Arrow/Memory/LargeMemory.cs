using System;
using System.Runtime.InteropServices;
using System.Buffers;
using System.Runtime.CompilerServices;
namespace Apache.Arrow.Memory
{
    public readonly struct LargeMemory<T> : IEquatable<LargeMemory<T>>
    {
        private readonly object _object;
        private readonly long _index;
        private readonly long _length;


        public LargeMemory(T[] array)
        {
            if (array == null)
            {
                this = default;
                return;
            }

            _object = array;
            _index = 0;
            _length = array.Length;
        }

        internal LargeMemory(T[] array, long start)
        {
            if (array == null)
            {
                this = default;
                return;
            }

            _object = array;
            _index = start;
            _length = array.Length - start;
        }

        public LargeMemory(T[] array, long start, long length)
        {
            if (array == null)
            {
                this = default;
                return;
            }

            _object = array;
            _index = start;
            _length = length;
        }

        internal LargeMemory(LargeMemoryManager<T> manager, long length)
        {
            if (length < 0)
                throw new ArgumentOutOfRangeException("length cannot be negative");

            _object = manager;
            _index = 0;
            _length = length;
        }

        internal LargeMemory(LargeMemoryManager<T> manager, long start, long length)
        {

            if (length < 0)
                throw new ArgumentOutOfRangeException("length cannot be negative");

            if (start < 0)
                throw new ArgumentOutOfRangeException("index cannot be negative");

            _object = manager;
            _index = start;
            _length = length;
        }

        internal LargeMemory(object obj, long start, long length)
        {
            _object = obj;
            _index = start;
            _length = length;
        }

        public static implicit operator LargeMemory<T>(T[] array) => new LargeMemory<T>(array);

        public static implicit operator LargeMemory<T>(ArraySegment<T> segment) => new LargeMemory<T>(segment.Array, segment.Offset, segment.Count);

        public static implicit operator ReadOnlyLargeMemory<T>(LargeMemory<T> memory) => new ReadOnlyLargeMemory<T>(memory.ToArray());

        public static LargeMemory<T> Empty => default;

        public long Length => _length;

        public bool IsEmpty => _length == 0;

        public LargeMemory<T> Slice(long start)
        {
            if(start>_length)
            {
                throw new ArgumentOutOfRangeException("start cannot exceed length");
            }

            return new LargeMemory<T>(_object, _index + start, _length - start);
        }

        public LargeMemory<T> Slice(long start, long length)
        {
            if(start+length>_length)
            {
                throw new ArgumentOutOfRangeException("length cannot exceed total remaining length");
            }

            return new LargeMemory<T>(_object, _index + start, length);
        }

        public unsafe LargeSpan<T> LargeSpan
        {
            get
            {
                ref T refToReturn = ref Unsafe.NullRef<T>();
                long lengthOfUnderlyingSpan = 0;

                object tmpObject = _object;

                if(tmpObject != null)
                {
                    if(tmpObject is T[] array)
                    {
                        refToReturn = ref MemoryMarshal.GetArrayDataReference(array);
                        lengthOfUnderlyingSpan = array.Length;
                    }else if(tmpObject is LargeMemoryManager<T> manager)
                    {
                        LargeSpan<T> memoryManagerSpan = manager.GetSpan();
                        refToReturn = ref memoryManagerSpan[0]; ;
                        lengthOfUnderlyingSpan = memoryManagerSpan.Length;
                    }
                    else
                    {
                        throw new InvalidOperationException("Unsupported object type.");
                    }

                    refToReturn = ref Unsafe.Add(ref refToReturn, (IntPtr)(void*)_index);
                    lengthOfUnderlyingSpan = _length;

                    return new LargeSpan<T>(ref refToReturn, lengthOfUnderlyingSpan);
                }
                else
                {
                    throw new InvalidOperationException("Object is null.");
                }

            }
        }

        public T[] ToArray() => LargeSpan.ToArray();

        public void CopyTo(LargeMemory<T> destination)
        {
            if(destination.Length < _length)
            {
                throw new ArgumentOutOfRangeException();
            }
            LargeSpan.CopyToLargeSpan(destination.LargeSpan);
        }

        public bool TryCopyTo(LargeMemory<T> destination)
        {
            if(destination.Length < _length)
            {
                return false;
            }

            LargeSpan.TryCopyTo(destination.LargeSpan);
            return true;
        }

        public unsafe MemoryHandle Pin()
        {
            object tmpObject = _object;

            if (tmpObject != null)
            {
                if (typeof(T) == typeof(char) && tmpObject is string s)
                {
                    GCHandle handle = GCHandle.Alloc(tmpObject, GCHandleType.Pinned);
                    fixed(char* pString = s)
                    {
                        char* ptr = pString + _index;
                        return new MemoryHandle((void*) ptr,handle);
                    }

                }
                else if (tmpObject is LargeMemoryManager<T> manager)
                {
                    LargeSpan<T> largeSpan = manager.GetSpan();
                    GCHandle handle = GCHandle.Alloc(tmpObject, GCHandleType.Pinned);
                    ref T firstElement = ref largeSpan[0];
                    void* pointer = Unsafe.AsPointer(ref firstElement);
                    return new MemoryHandle(pointer, handle);

                } else
                {
                    throw new InvalidOperationException("Unsupported object type for pinning.");
                }
            }

            return default;
        }

        public override bool Equals(object obj)
        {
            if(obj is LargeMemory<T> memory)
            {
                return Equals(memory);
            }
            else
            {
                return false;
            }
        }

        public bool Equals(LargeMemory<T> other)
        {
            return
                _object == other._object &&
                _index == other._index &&
                _length == other._length;
        }

        public override int GetHashCode()
        {
            return (_object != null ) ? HashCode.Combine(RuntimeHelpers.GetHashCode(_object), _index, _length) : 0;
        }


    }

}
