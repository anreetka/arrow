using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Apache.Arrow.Memory
{
    public readonly struct ReadOnlyLargeMemory<T>: IEquatable<ReadOnlyLargeMemory<T>>
    {
        private readonly object _object;
        private readonly long _index;
        private readonly long _length;

        public ReadOnlyLargeMemory(T[] array)
        {
            if (array == null)
            {
                this = default;
                return; // returns default
            }

            _object = array;
            _index = 0;
            _length = array.Length;
        }


        public ReadOnlyLargeMemory(T[] array, long start, long length)
        {
            if (array == null)
            {
                if (start != 0 || length != 0)
                    throw new ArgumentOutOfRangeException();

                this = default;
                return;
            }

            if (start < 0 || length < 0 || start + length > array.Length)
                throw new ArgumentOutOfRangeException();

            _object = array;
            _index = start;
            _length = length;
        }

        internal ReadOnlyLargeMemory(object obj, long start, long length)
        {
            _object = obj;
            _index = start;
            _length = length;
        }
        public static implicit operator ReadOnlyLargeMemory<T>(T[] array) => new ReadOnlyLargeMemory<T>(array);
        public static implicit operator ReadOnlyLargeMemory<T>(ArraySegment<T> segment) => new ReadOnlyLargeMemory<T>(segment.Array, segment.Offset, segment.Count);
        public static ReadOnlyMemory<T> Empty => default;
        public long Length => _length;

        public bool IsEmpty => _length == 0;

        public ReadOnlyLargeMemory<T> Slice(long start)
        {
            if (start > _length)
                throw new ArgumentOutOfRangeException();

            return new ReadOnlyLargeMemory<T>(_object, _index + start, _length - start);
        }

        public ReadOnlyLargeMemory<T> Slice(long start, long length)
        {
            if (start < 0 || length < 0 || start + length > _length)
                throw new ArgumentOutOfRangeException();

            return new ReadOnlyLargeMemory<T>(_object, _index + start, length);
        }


        public unsafe ReadOnlyLargeSpan<T> Span
        {
            get
            {
                ref T refToReturn = ref Unsafe.NullRef<T>();
                long lengthOfUnderlyingSpan = 0;

                object tmpObject = _object;

                if (tmpObject != null)
                {
                    if (tmpObject is T[] array)
                    {
                        refToReturn = ref MemoryMarshal.GetArrayDataReference(array);
                        lengthOfUnderlyingSpan = array.Length;
                    }
                    else if (tmpObject is ReadOnlyLargeMemory<T> largeMemory)
                    {
                        refToReturn = ref largeMemory.GetReference();
                        lengthOfUnderlyingSpan = largeMemory.Length;
                    }
                }

                return new ReadOnlyLargeSpan<T>(ref refToReturn, lengthOfUnderlyingSpan);
            }
        }

        private ref T GetReference()
        {
            if (_object is T[] array)
            {
                return ref array[_index];
            }

            throw new InvalidOperationException("Invalid object type.");
        }

        public void CopyTo(LargeMemory<T> destination) => Span.CopyTo(destination.LargeSpan);
        public bool TryCopyTo(LargeMemory<T> destination) => Span.TryCopyTo(destination.LargeSpan);

        public unsafe MemoryHandle Pin()
        {
            object tmpObject = _object;

            if (tmpObject != null)
            {
                if (tmpObject is T[] array)
                {
                    GCHandle handle = GCHandle.Alloc(tmpObject, GCHandleType.Pinned);
                    void* pointer = Unsafe.Add<T>(Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(array)), (int)_index);
                    return new MemoryHandle(pointer, handle);
                }
                else if (tmpObject is LargeMemoryManager<T> memoryManager)
                {
                    return memoryManager.Pin(_index);
                }
            }

            return default;
        }


        public T[] ToArray() => Span.ToArray();

        public bool Equals(ReadOnlyLargeMemory<T> other)
        {
            return _object == other._object && _index == other._index && _length == other._length;
        }

        public override bool Equals(object obj)
        {
            return obj is ReadOnlyLargeMemory<T> memory && Equals(memory);
        }

        public override int GetHashCode()
        {
            return (_object != null) ? HashCode.Combine(RuntimeHelpers.GetHashCode(_object), _index, _length) : 0;
        }

        public override string ToString()
        {
            return $"System.ReadOnlyLargeMemory<{typeof(T).Name}>[{_length}]";
        }

        internal object GetObjectStartLength(out long start, out long length)
        {
            start = _index;
            length = _length;
            return _object;
        }


    }
}
