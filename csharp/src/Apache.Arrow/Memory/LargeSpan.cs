using System;
using System.Buffers;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Apache.Arrow.Memory
{
    public readonly ref struct LargeSpan<T>
    {
        internal readonly ref T _reference;
        private readonly long _length;
        public LargeSpan(T[] array)
        {
            if(array == null)
            {
                this = default;
                return;
            }

            if (!typeof(T).IsValueType && array.GetType() != typeof(T[]))
                throw new ArrayTypeMismatchException(nameof(array));

            _reference = ref MemoryMarshal.GetArrayDataReference(array);
            _length = array.Length;
        }

        public LargeSpan(T[] array, long start, long length)
        {
            if(array == null)
            {
                if (start != 0 || length!= 0)
                {
                    throw new ArgumentOutOfRangeException();
                }

                this = default;
                return;
            }

            if(start + length> array.Length)
            {
                throw new ArgumentOutOfRangeException();
            }

            _reference = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(array), (nint)start);
            _length = length;
        }

        public unsafe LargeSpan(void* pointer, long length)
        {
           if(length < 0)
           {
                throw new ArgumentOutOfRangeException();
           }

            if (pointer == null)
            {
                throw new ArgumentNullException(nameof(pointer), "Pointer cannot be null.");
            }

            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            {
                throw new InvalidOperationException($"Pointers are not supported for reference types or types containing references, like {typeof(T)}.");
            }

            _reference = ref Unsafe.AsRef<T>(pointer);
            _length = length;
        }

        public LargeSpan(ref T reference)
        {
            _reference = ref reference;
            _length = 1;
        }

        internal LargeSpan(ref T reference, long length)
        {
            _reference = ref reference;
            _length = length;
        }

        public ref T this[long index]
        {
            get
            {
                if(index > _length)
                {
                    throw new ArgumentOutOfRangeException();
                }

                return ref Unsafe.Add(ref _reference, (nint)index);
            }
        }

        public long Length => _length;

        public bool IsEmpty() => _length == 0;

        public override bool Equals(object obj)
        {
            throw new NotSupportedException("The Equals method is not supported on Span.");
        }

        public override int GetHashCode()
        {
            throw new NotSupportedException("The GetHashCode method is not supported on Span.");
        }

        public static implicit operator LargeSpan<T>(T[] array) => new LargeSpan<T>(array);

        public static implicit operator LargeSpan<T>(ArraySegment<T> segment) =>
           new LargeSpan<T>(segment.Array, segment.Offset, segment.Count);

        public static LargeSpan<T> Empty => default;

        public ref T GetPinnableReference()
        {
            ref T ret = ref Unsafe.NullRef<T>();
            if (_length != 0) ret = ref _reference;
            return ref ret;
        }

        public unsafe void Clear()
        {
            if (_length == 0)
                return;

            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            {
                IntPtr* pointer = (IntPtr*)Unsafe.AsPointer(ref _reference);

                for (long i = 0; i < _length; i++)
                {
                    pointer[i] = IntPtr.Zero; 
                }
            }
            else
            {
                byte* pointer = (byte*)Unsafe.AsPointer(ref _reference);

                for (long i = 0; i < _length * Unsafe.SizeOf<T>(); i++)
                {
                    pointer[i] = 0;
                }
            }
        }

        public unsafe void CopyToLargeSpan(LargeSpan<T> destination)
        {
            if(_length > destination.Length)
            {
                throw new ArgumentException("Destination is too short");
            }

            if (_length == 0)
                return;

            long byteCount = _length * Unsafe.SizeOf<T>();

            Buffer.MemoryCopy(Unsafe.AsPointer(ref _reference), Unsafe.AsPointer(ref destination._reference),  byteCount, byteCount);
        }

        public unsafe bool TryCopyTo(LargeSpan<T> destination)
        {
            bool retVal = false;
            if(_length <= destination._length)
            {
                long byteCount = _length * Unsafe.SizeOf<T>();

                Buffer.MemoryCopy(Unsafe.AsPointer(ref _reference), Unsafe.AsPointer(ref destination._reference), byteCount, byteCount);
                retVal = true;
            }

            return retVal;
        }
        public unsafe void Fill(T value)
        {
            if (_length == 0)
                return;

            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            {
                IntPtr* pointer = (IntPtr*)Unsafe.AsPointer(ref _reference);

                for (long i = 0; i < _length; i++)
                {
                    pointer[i] = (IntPtr)(object)value;
                }
            }
            else
            {
                byte* pointer = (byte*)Unsafe.AsPointer(ref _reference);

                for (long i = 0; i < _length; i++)
                {
                    ref T element = ref Unsafe.Add(ref _reference, (nint)i);
                    element = value; 
                }
            }
        }

        public LargeSpan<T> Slice(long start)
        {
            if(start > _length)
            {
                throw new ArgumentOutOfRangeException();
            }

            ref T newReference = ref Unsafe.Add(ref _reference, (nint)start);
            return new LargeSpan<T>(ref newReference, _length-start);
        }

        public LargeSpan<T> Slice(long start, long length)
        {
            if(start+length>_length)
            {
                throw new ArgumentOutOfRangeException();
            }

            if(start>_length || length > (_length - start))
            {
                throw new ArgumentOutOfRangeException();
            }

            return new LargeSpan<T>(ref Unsafe.Add(ref _reference, (nint) start), length);
        }

        public T[] ToArray()
        {
            if (_length == 0)
                return new T[0];

            var destination = new T[_length];

            unsafe
            {
                ref T destinationPointer = ref destination[0];
                ref T sourcePointer = ref _reference;

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
