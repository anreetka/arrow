using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.InteropServices;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Diagnostics;
using System.Text.Unicode;
using System.Diagnostics.CodeAnalysis;

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

        public static implicit operator ReadOnlyMemory<T>(LargeMemory<T> memory) => new ReadOnlyLargeMemory<T>(memory.ToArray());

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

        public Span<T> Span
        {
            get
            {
                if(_object is T[] array)
                {
                    //implement here
                }else if(_object is LargeMemoryManager<T> manager)
                {
                   //implement here
                }
                else
                {
                    throw new InvalidOperationException("Unsupported object type.");
                }
            }
        }

        public void CopyTo(LargeMemory<T> destination)
        {
            if(destination.Length < _length)
            {
                throw new ArgumentOutOfRangeException();
            }

            Span.CopyTo(destination.Span);
        }

        public bool TryCopyTo(LargeMemory<T> destination)
        {
            if(destination.Length < _length)
            {
                return false;
            }

            Span.CopyTo(destination.Span);
            return true;
        }

        public unsafe MemoryHandle Pin()
        {
            if(_object is T[] array)
            {
                //implement here
            }
            else if (_object is LargeMemoryManager<T> manager)
            {
                return manager.Pin(); 
            }
            else
            {
                throw new InvalidOperationException("Pinning is not supported for this memory type.");
            }
        }

        public T[] ToArray() => Span.ToArray();

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
