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
using Apache.Arrow.Arrays;
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

            if (Schema == null || Schema.FieldsList == null)
            {
                return "Schema is not defined.";
            }

            if (ColumnCount != Schema.FieldsList.Count)
            {
                return "Column count does not match the schema fields.";
            }

            sb.AppendLine("=== Schema Information ===");

            for (int i = 0; i < ColumnCount; i++)
            {
                var field = Schema.FieldsList[i];
                sb.AppendLine($"{field.Name}: {field.DataType.GetType().Name}");
            }
            sb.AppendLine("==========================");

            sb.AppendLine("=== Column Data ===");

            for (int i = 0; i < ColumnCount; i++)
            {
                var column = Column(i);
                var field = Schema.FieldsList[i];

                sb.Append($"{field.Name}:[");

                var visitor = new ArrayPrinter(sb);
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
}
