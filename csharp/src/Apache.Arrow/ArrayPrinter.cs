﻿using System;
using System.Collections.Generic;
using System.Text;
using Apache.Arrow.Arrays;

namespace Apache.Arrow
{
    internal class ArrayPrinter :
        IArrowArrayVisitor<IArrowArray>,
        IArrowArrayVisitor<BinaryArray>,
        IArrowArrayVisitor<BooleanArray>,
        IArrowArrayVisitor<Date32Array>,
        IArrowArrayVisitor<Date64Array>,
        IArrowArrayVisitor<Decimal128Array>,
        IArrowArrayVisitor<Decimal256Array>,
        IArrowArrayVisitor<DoubleArray>,
        IArrowArrayVisitor<DurationArray>,
        IArrowArrayVisitor<FixedSizeBinaryArray>,
        IArrowArrayVisitor<FloatArray>,
        IArrowArrayVisitor<Int16Array>,
        IArrowArrayVisitor<Int32Array>,
        IArrowArrayVisitor<Int64Array>,
        IArrowArrayVisitor<Int8Array>,
        IArrowArrayVisitor<LargeBinaryArray>,
        IArrowArrayVisitor<LargeStringArray>,
        IArrowArrayVisitor<StringArray>,
        IArrowArrayVisitor<StringViewArray>,
        IArrowArrayVisitor<Time32Array>,
        IArrowArrayVisitor<Time64Array>,
        IArrowArrayVisitor<TimestampArray>,
        IArrowArrayVisitor<UInt16Array>,
        IArrowArrayVisitor<UInt32Array>,
        IArrowArrayVisitor<UInt64Array>,
        IArrowArrayVisitor<UInt8Array>,
        IArrowArrayVisitor<UnionArray>
    {
        private readonly StringBuilder _sb = new StringBuilder();
        public ArrayPrinter(StringBuilder sb)
        {
            _sb = sb ?? throw new ArgumentNullException(nameof(sb));
        }

        private void VisitGeneric<T>(T array, Func<int, object>getValue) where T: IArrowArray
        {
            PrintArray(array, getValue);
        }
        public void Visit(IArrowArray array) => _sb.Append($"Unsupported array type for {array.GetType().Name}");
        public void Visit(BinaryArray array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : Convert.ToBase64String(array.GetBytes(i).ToArray()));
        public void Visit(BooleanArray array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(Date32Array array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(Date64Array array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(Decimal128Array array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(Decimal256Array array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));

        public void Visit(DictionaryArray array)
        {
            PrintArray(array, i =>
            {
                if (array.IsNull(i)) return "NULL";

                var index = ((Int32Array)array.Indices).GetValue(i);
                var dictValue = ((StringArray)array.Dictionary).IsNull((int)index) ? "NULL" : PrintString(((StringArray)array.Dictionary).GetString((int)index));

                return $"[{index}, {dictValue}]";
            });
        }
        public void Visit(DoubleArray array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(DurationArray array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(FixedSizeBinaryArray array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : Convert.ToBase64String(array.GetBytes(i).ToArray()));
        public void Visit(FloatArray array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(Int16Array array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(Int32Array array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(Int64Array array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(Int8Array array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(LargeBinaryArray array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : Convert.ToBase64String(array.GetBytes(i).ToArray()));
        public void Visit(LargeStringArray array) => VisitGeneric(array, i => PrintString(array.GetString(i)));
        public void Visit(StringArray array) => VisitGeneric(array, i => PrintString(array.GetString(i)));
        public void Visit(StringViewArray array) => VisitGeneric(array, i => PrintString(array.GetString(i)));
        public void Visit(Time32Array array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(Time64Array array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(TimestampArray array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(UInt16Array array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(UInt32Array array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(UInt64Array array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(UInt8Array array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : array.GetValue(i));
        public void Visit(UnionArray array) => VisitGeneric(array, i => array.IsNull(i) ? "NULL" : $"Union[{i}]");

        private string PrintString(string value) => value != null ? $"\"{value}\"" : "NULL";

        private void AppendElements(IArrowArray array, Func<int, object> getValue, int start, int end)
        {
            for (int i = start; i < end; i++)
            {
                _sb.Append(getValue(i)?.ToString());
                if (i < end - 1)
                {
                    _sb.Append(",");
                }
            }
        }

        private void PrintArray(IArrowArray array, Func<int, object> getValue)
        {
            if (array == null)
            {
                _sb.Append("[NULL ARRAY]");
                return;
            }

            if (array.Length == 0)
            {
                _sb.Append("[EMPTY ARRAY]");
                return;
            }

            _sb.Append("[");

            if (array.Length < 12)
            {
                AppendElements(array, getValue, 0, array.Length);
            }
            else
            {
                AppendElements(array, getValue, 0, 5);
                _sb.Append(",...,");
                AppendElements(array, getValue, array.Length - 5, array.Length);
            }

            _sb.Append("]");
        }

    }

}
