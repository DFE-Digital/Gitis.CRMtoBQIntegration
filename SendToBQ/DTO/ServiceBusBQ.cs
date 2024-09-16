using System.Collections.Generic;

namespace SendToBQ.DTO
{
    public class Field
    {
        public string Key { get; set; }
        public object Value { get; set; }
    }

    public class ServiceBusBQ
    {
        public string MessageType { get; set; }
        public string Id { get; set; }
        public string LogicalName { get; set; }
        public List<Field> Fields { get; set; }
    }
}
