using Microsoft.Azure.Cosmos.Table;
using System;

namespace FreedomTools
{
    public class AlmostTable: TableEntity
    {
        public AlmostTable()
        {
        }

        public AlmostTable(string messageId)
        {
            PartitionKey = GetTimelyKey();
            RowKey = messageId;
        }


        public static string GetTimelyKey(int offset = 0)
        {
            var date = DateTime.Now.Year * 100 + DateTime.Now.Month + offset;
            return date.ToString();
        }
    }
}