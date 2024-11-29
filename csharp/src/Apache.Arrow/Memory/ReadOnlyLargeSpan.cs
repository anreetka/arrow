using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Apache.Arrow.Memory
{
    public readonly ref struct ReadOnlyLargeSpan<T>
    {
        internal readonly ref T _pointer;
        private readonly long _length;

        public ReadOnlyLargeSpan(T[] array)
        {
            if (array == null)
            {
                this = default;
                return;
            }

            _pointer = ref MemoryMarshal.GetArrayDataReference(array);
            _length = array.Length;
        }

        public ReadOnlyLargeSpan(T[] array, long start, long length)
        {
            if (array == null)
            {
                throw new ArgumentNullException(nameof(array), "Array cannot be null.");
            }

            if (start < 0 || start >= array.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(start), "Start index must be within the bounds of the array.");
            }

            if (length < 0 || start + length > array.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(length), "The length must be non-negative and fit within the array from the start index.");
            }

            _pointer = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(array), (int)start);
            _length = length;
        }

        public unsafe ReadOnlyLargeSpan(void* pointer, long length)
        {
            if (length < 0)
                throw new ArgumentOutOfRangeException();

            _pointer = ref Unsafe.As<byte, T>(ref *(byte*)pointer);
            _length = length;
        }

        internal ReadOnlyLargeSpan(ref T ptr, long length)
        {
            _pointer = ref ptr;
            _length = length;
        }

        public ref readonly T this[long index]
        {
            get
            {
                if (index >= _length)
                    throw new ArgumentOutOfRangeException();

                return ref Unsafe.Add(ref _pointer, (int)index);
            }
        }

        public long Length => _length;
        public bool IsEmpty => _length == 0;

        public override bool Equals(object obj) =>
            throw new InvalidOperationException("The Equals method is not supported for ReadOnlyLargeSpan.");

        public override int GetHashCode() =>
            throw new InvalidOperationException("The GetHasCode method is not supported for ReadOnlyLargeSpan.");

        public static implicit operator ReadOnlyLargeSpan<T>(T[] array) => new ReadOnlyLargeSpan<T>(array);
        public static implicit operator ReadOnlyLargeSpan<T>(ArraySegment<T> segment)
            => new ReadOnlyLargeSpan<T>(segment.Array, segment.Offset, segment.Count);

        public static ReadOnlyLargeSpan<T> Empty => default;

        public ref readonly T GetPinnableReference()
        {
            ref T ret = ref Unsafe.NullRef<T>();
            if (_length != 0) ret = ref _pointer;
            return ref ret;
        }

        public void CopyTo(LargeSpan<T> destination)
        {

            if (_length <= destination.Length)
            {
                unsafe
                {
                    ref T destinationPointer = ref destination._reference;
                    ref T sourcePointer = ref _pointer;

                    nuint byteLength = (nuint)(_length * Unsafe.SizeOf<T>());


                    Buffer.MemoryCopy(
                        Unsafe.AsPointer(ref sourcePointer),   
                        Unsafe.AsPointer(ref destinationPointer),
                        byteLength,                          
                        byteLength                           
                    );
                }
            }
            else
            {
                throw new InvalidOperationException("Destination is too short");
            }
        }

        public bool TryCopyTo(LargeSpan<T> destination)
        {
            if (_length <= destination.Length)
            {
                unsafe
                {
                    ref T destinationPointer = ref destination._reference;
                    ref T sourcePointer = ref _pointer;

                    nuint byteLength = (nuint)(_length * Unsafe.SizeOf<T>());

                    Buffer.MemoryCopy(
                        Unsafe.AsPointer(ref sourcePointer),   
                        Unsafe.AsPointer(ref destinationPointer),
                        byteLength,                           
                        byteLength                             
                    );
                }
                return true;
            }
            return false;
        }

        public override string ToString()
        {
            return $"ReadOnlyLargeSpan<{typeof(T).Name}>[{_length}]";
        }

        public ReadOnlyLargeSpan<T> Slice(long start)
        {
            if (start > _length)
                throw new ArgumentOutOfRangeException();

            return new ReadOnlyLargeSpan<T>(ref Unsafe.Add(ref _pointer, (int)start), _length - start);
        }

        public ReadOnlyLargeSpan<T> Slice(long start, long length)
        {
            if (start > _length || length > (_length - start))
                throw new ArgumentOutOfRangeException();

            return new ReadOnlyLargeSpan<T>(ref Unsafe.Add(ref _pointer, (int)start), length);
        }

        public T[] ToArray()
        {
            if (_length == 0)
                return new T[0];

            var destination = new T[_length];

            unsafe
            {
                ref T destinationPointer = ref destination[0];
                ref T sourcePointer = ref _pointer;

                nuint byteLength = (nuint)(_length * Unsafe.SizeOf<T>());

                Buffer.MemoryCopy(
                    Unsafe.AsPointer(ref sourcePointer),   
                    Unsafe.AsPointer(ref destinationPointer), 
                    byteLength,                            
                    byteLength                            
                );
            }

            return destination;
        }

    }
}
