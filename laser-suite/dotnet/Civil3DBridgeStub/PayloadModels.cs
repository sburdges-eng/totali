using System.Collections.Generic;

namespace Civil3DBridgeStub;

public sealed class RppRow
{
    public string PairId { get; set; } = string.Empty;
    public string StationI { get; set; } = string.Empty;
    public string StationJ { get; set; } = string.Empty;
    public double DistanceM { get; set; }
    public double RppActualM { get; set; }
    public double RppAllowableM { get; set; }
    public bool Compliant { get; set; }
}

public sealed class Item20Row
{
    public string ItemId { get; set; } = string.Empty;
    public string ConditionType { get; set; } = string.Empty;
    public string LocationReference { get; set; } = string.Empty;
    public double Magnitude { get; set; }
    public string Units { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
}

public sealed class TopologicPayload
{
    public string SchemaVersion { get; set; } = "1.0.0";
    public List<RppRow> RppRows { get; set; } = new();
    public List<Item20Row> Item20Rows { get; set; } = new();
    public bool NotificationRequired { get; set; }
}
