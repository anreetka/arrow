// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Apache.Arrow.Flatbuf;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public partial class RecordBatch : IArrowRecord
    {
        public Schema Schema { get; }
        public int ColumnCount => _arrays.Count;
        public IEnumerable<IArrowArray> Arrays => _arrays;
        public int Length { get; }

        internal IReadOnlyList<IArrowArray> ArrayList => _arrays;

        private readonly IMemoryOwner<byte> _memoryOwner;
        private readonly List<IArrowArray> _arrays;

        public IArrowArray Column(int i)
        {
            return _arrays[i];
        }

        public IArrowArray Column(string columnName)
        {
            return Column(columnName, null);
        }

        public IArrowArray Column(string columnName, IEqualityComparer<string> comparer)
        {
            int fieldIndex = Schema.GetFieldIndex(columnName, comparer);
            return _arrays[fieldIndex];
        }

        public void Dispose()
        {
            Dispose(disposing: true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _memoryOwner?.Dispose();

                foreach (IArrowArray array in _arrays)
                {
                    array.Dispose();
                }
            }
        }

        public RecordBatch(Schema schema, IEnumerable<IArrowArray> data, int length)
        {
            if (length < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            _arrays = data?.ToList() ?? throw new ArgumentNullException(nameof(data));

            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            Length = length;
        }

        internal RecordBatch(Schema schema, IMemoryOwner<byte> memoryOwner, List<IArrowArray> arrays, int length)
        {
            Debug.Assert(schema != null);
            Debug.Assert(arrays != null);
            Debug.Assert(length >= 0);

            _memoryOwner = memoryOwner;
            _arrays = arrays;
            Schema = schema;
            Length = length;
        }

        public RecordBatch Clone(MemoryAllocator allocator = default)
        {
            IEnumerable<IArrowArray> arrays = _arrays.Select(array => ArrowArrayFactory.BuildArray(array.Data.Clone(allocator)));
            return new RecordBatch(Schema, arrays, Length);
        }

        public RecordBatch Slice(int offset, int length)
        {
            if (offset > Length)
            {
                throw new ArgumentException($"Offset {offset} cannot be greater than Length {Length} for RecordBatch.Slice");
            }

            length = Math.Min(Length - offset, length);
            return new RecordBatch(Schema, _arrays.Select(a => ArrowArrayFactory.Slice(a, offset, length)), length);
        }

        public void Accept(IArrowArrayVisitor visitor)
        {
            switch (visitor)
            {
                case IArrowArrayVisitor<RecordBatch> recordBatchVisitor:
                    recordBatchVisitor.Visit(this);
                    break;
                case IArrowArrayVisitor<IArrowRecord> arrowStructVisitor:
                    arrowStructVisitor.Visit(this);
                    break;
                default:
                    visitor.Visit(this);
                    break;
            }
        }

        public string PrettyPrint()
        {
            var sb = new StringBuilder();

            for (int i = 0; i < ColumnCount; i++)
            {
                var fieldType = Schema.FieldsList[i];
                sb.AppendLine($"{fieldType.Name}: {fieldType.DataType.GetType().Name}");
            }
            sb.AppendLine("----");

            for (int i = 0; i < ColumnCount; i++)
            {
                var column = Column(i);
                var fieldType = Schema.FieldsList[i];

                sb.Append($"{fieldType.Name}:[");

                var visitor = new PrettyPrintVisitor(sb);
                column.Accept(visitor);

                sb.AppendLine("]"); 
            }

            return sb.ToString();
        }



        public override string ToString() => $"{nameof(RecordBatch)}: {ColumnCount} columns by {Length} rows";

        IRecordType IArrowRecord.Schema => this.Schema;
        int IArrowArray.NullCount => 0;
        int IArrowArray.Offset => 0;
        ArrayData IArrowArray.Data => throw new NotSupportedException("Unable to get data for RecordBatch");

        bool IArrowArray.IsNull(int index) => false;
        bool IArrowArray.IsValid(int index) => true;
    }

    public class PrettyPrintVisitor : IArrowArrayVisitor<IArrowArray>
    {
        private readonly StringBuilder _sb = new StringBuilder();

        public PrettyPrintVisitor(StringBuilder sb)
        {
            _sb = sb;
        }
        public void Visit(IArrowArray array)
        {
            if (array is Int32Array int32Array)
            {
                Visit(int32Array);
            }
            else if (array is StringArray stringArray)
            {
                Visit(stringArray);
            }else if(array is Int64Array int64Array)
            {
                Visit(int64Array);
            }
            else if (array is DoubleArray doubleArray)
            {
                Visit(doubleArray);
            }
            else if (array is FloatArray floatArray)
            {
                Visit(floatArray);
            }
            else if (array is BooleanArray booleanArray)
            {
                Visit(booleanArray);
            }
            else if (array is UInt16Array uint16Array)
            {
                Visit(uint16Array);
            }
            else if (array is UInt32Array uint32Array)
            {
                Visit(uint32Array);
            }
            else if (array is UInt64Array uint64Array)
            {
                Visit(uint64Array);
            }
            else if (array is Date32Array date32Array)
            {
                Visit(date32Array);
            }
            else if (array is Date64Array date64Array)
            {
                Visit(date64Array);
            }
            else if (array is TimestampArray timestampArray)
            {
                Visit(timestampArray);
            }
            else if (array is Time32Array time32Array)
            {
                Visit(time32Array);
            }
            else if (array is Time64Array time64Array)
            {
                Visit(time64Array);
            }
            else
            {
                _sb.AppendLine("Unsupported array type.");
            }
        }

        private void PrintArray<T>(IArrowArray array, Func<int, T> getValue)
        {
            _sb.Append("[");

            if(array.Length < 12)
            {
                for (int i = 0; i < array.Length; i++)
                {
                    _sb.Append(array.IsNull(i) ? "NULL" : getValue(i)?.ToString());
                    if (i < array.Length - 1)
                    {
                        _sb.Append(",");
                    }

                }
            }
            else
            {
                for (int i = 0; i < 5; i++)
                {
                    _sb.Append(array.IsNull(i) ? "NULL" : getValue(i)?.ToString());
                    if (i < 4)
                    {
                        _sb.Append(",");
                    }
                }

                _sb.Append(",...,");

               for (int i = array.Length - 5; i <array.Length; i++)
               {
                   _sb.Append(array.IsNull(i) ? "NULL" : getValue(i)?.ToString());
                   if (i < array.Length - 1) 
                   {
                        _sb.Append(",");
                   }
               }
            }

            _sb.Append("]");
        }


        public void Visit(Int32Array array)
        {
            PrintArray(array, array.GetValue);
        }

        public void Visit(Int64Array array)
        {
            PrintArray(array, array.GetValue);
        }

        public void Visit(DoubleArray array)
        {
            PrintArray(array, array.GetValue);
        }

        public void Visit(StringArray array)
        {
            PrintArray(array, i => array.IsNull(i) ? "NULL" : $"\"{array.GetString(i)}\"");
        }

        public void Visit(FloatArray array)
        {
            PrintArray(array, array.GetValue);
        }

        public void Visit(BooleanArray array)
        {
            PrintArray(array, array.GetValue);
        }
        public void Visit(UInt16Array array)
        {
            PrintArray(array, array.GetValue);
        }
        public void Visit(UInt32Array array)
        {
            PrintArray(array, array.GetValue);
        }
        public void Visit(UInt64Array array)
        {
            PrintArray(array, array.GetValue);
        }
        public void Visit(Date32Array array)
        {
            PrintArray(array, array.GetValue);
        }
        public void Visit(Date64Array array)
        {
            PrintArray(array, array.GetDateTime);
        }
        public void Visit(TimestampArray array)
        {
            PrintArray(array, array.GetTimestamp);
        }
        public void Visit(Time32Array array)
        {
            PrintArray(array, array.GetValue);
        }
        public void Visit(Time64Array array)
        {
            PrintArray(array, array.GetValue);
        }
    }
}
