using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;
using CommandLine;
using Mapster.Common;
using Mapster.Common.MemoryMappedTypes;
using OSMDataParser;
using OSMDataParser.Elements;

namespace MapFeatureGenerator;

public static class Program
{
    private static MapData LoadOsmFile(ReadOnlySpan<char> osmFilePath)
    {
        var nodes = new ConcurrentDictionary<long, AbstractNode>();
        var ways = new ConcurrentBag<Way>();

        Parallel.ForEach(new PBFFile(osmFilePath), (blob, _) =>
        {
            switch (blob.Type)
            {
                case BlobType.Primitive:
                    {
                        var primitiveBlock = blob.ToPrimitiveBlock();
                        foreach (var primitiveGroup in primitiveBlock)
                            switch (primitiveGroup.ContainedType)
                            {
                                case PrimitiveGroup.ElementType.Node:
                                    foreach (var node in primitiveGroup) nodes[node.Id] = (AbstractNode)node;
                                    break;

                                case PrimitiveGroup.ElementType.Way:
                                    foreach (var way in primitiveGroup) ways.Add((Way)way);
                                    break;
                            }

                        break;
                    }
            }
        });

        var tiles = new Dictionary<int, List<long>>();
        foreach (var (id, node) in nodes)
        {
            var tileId = TiligSystem.GetTile(new Coordinate(node.Latitude, node.Longitude));
            if (tiles.TryGetValue(tileId, out var nodeIds))
            {
                nodeIds.Add(id);
            }
            else
            {
                tiles[tileId] = new List<long>
                {
                    id
                };
            }
        }

        return new MapData
        {
            Nodes = nodes.ToImmutableDictionary(),
            Tiles = tiles.ToImmutableDictionary(),
            Ways = ways.ToImmutableArray()
        };
    }

    private static void CreateMapDataFile(ref MapData mapData, string filePath)
    {
        var usedNodes = new HashSet<long>();

        long featureIdCounter = -1;
        var featureIds = new List<long>();
        // var geometryTypes = new List<GeometryType>();
        // var coordinates = new List<(long id, (int offset, List<Coordinate> coordinates) values)>();

        var labels = new List<int>();
        // var propKeys = new List<(long id, (int offset, IEnumerable<string> keys) values)>();
        // var propValues = new List<(long id, (int offset, IEnumerable<string> values) values)>();

        using var fileWriter = new BinaryWriter(File.OpenWrite(filePath));
        var offsets = new Dictionary<int, long>(mapData.Tiles.Count);

        // Write FileHeader
        fileWriter.Write((long)1); // FileHeader: Version
        fileWriter.Write(mapData.Tiles.Count); // FileHeader: TileCount

        // Write TileHeaderEntry
        foreach (var tile in mapData.Tiles)
        {
            fileWriter.Write(tile.Key); // TileHeaderEntry: ID
            fileWriter.Write((long)0); // TileHeaderEntry: OffsetInBytes
        }

        foreach (var (tileId, _) in mapData.Tiles)
        {
            // FIXME: Not thread safe
            usedNodes.Clear();

            // FIXME: Not thread safe
            featureIds.Clear();
            labels.Clear();

            var totalCoordinateCount = 0;
            var totalPropertyCount = 0;

            var featuresData = new Dictionary<long, FeatureData>();

            foreach (var way in mapData.Ways)
            {
                var featureId = Interlocked.Increment(ref featureIdCounter);

                var featureData = new FeatureData
                {
                    Id = featureId,
                    Coordinates = (totalCoordinateCount, new List<Coordinate>()),
                    PropertyKeys = (totalPropertyCount, new List<int>(way.Tags.Count)),
                    PropertyValues = (totalPropertyCount, new List<int>(way.Tags.Count))
                };

                var geometryType = GeometryType.Polyline;

                labels.Add(-1);
                foreach (var tag in way.Tags)
                {
                    if (tag.Key == "name")
                    {
                        labels[^1] = totalPropertyCount * 2 + featureData.PropertyKeys.keys.Count * 2 + 1;
                    }
                    featureData.PropertyKeys.keys.Add(ConvertKey(tag.Key));
                    featureData.PropertyValues.values.Add(ConvertValue(tag.Key, tag.Value));
                }

                foreach (var nodeId in way.NodeIds)
                {
                    var node = mapData.Nodes[nodeId];
                    if (TiligSystem.GetTile(new Coordinate(node.Latitude, node.Longitude)) != tileId)
                    {
                        continue;
                    }

                    usedNodes.Add(nodeId);

                    foreach (var (key, value) in node.Tags)
                    {
                        if (!featureData.PropertyKeys.keys.Contains(ConvertKey(key)))
                        {
                            featureData.PropertyKeys.keys.Add(ConvertKey(key));
                            featureData.PropertyValues.values.Add(ConvertValue(key, value));
                        }
                    }

                    featureData.Coordinates.coordinates.Add(new Coordinate(node.Latitude, node.Longitude));
                }

                // This feature is not located within this tile, skip it
                if (featureData.Coordinates.coordinates.Count == 0)
                {
                    // Remove the last item since we added it preemptively
                    labels.RemoveAt(labels.Count - 1);
                    continue;
                }

                if (featureData.Coordinates.coordinates[0] == featureData.Coordinates.coordinates[^1])
                {
                    geometryType = GeometryType.Polygon;
                }
                featureData.GeometryType = (byte)geometryType;

                totalPropertyCount += featureData.PropertyKeys.keys.Count;
                totalCoordinateCount += featureData.Coordinates.coordinates.Count;

                if (featureData.PropertyKeys.keys.Count != featureData.PropertyValues.values.Count)
                {
                    throw new InvalidDataContractException("Property keys and values should have the same count");
                }

                featureIds.Add(featureId);
                featuresData.Add(featureId, featureData);
            }

            foreach (var (nodeId, node) in mapData.Nodes.Where(n => !usedNodes.Contains(n.Key)))
            {
                if (TiligSystem.GetTile(new Coordinate(node.Latitude, node.Longitude)) != tileId)
                {
                    continue;
                }

                var featureId = Interlocked.Increment(ref featureIdCounter);

                var featurePropKeys = new List<int>();
                var featurePropValues = new List<int>();

                labels.Add(-1);
                for (var i = 0; i < node.Tags.Count; ++i)
                {
                    var tag = node.Tags[i];
                    if (tag.Key == "name")
                    {
                        labels[^1] = totalPropertyCount * 2 + featurePropKeys.Count * 2 + 1;
                    }

                    featurePropKeys.Add(ConvertKey(tag.Key));
                    featurePropValues.Add(ConvertValue(tag.Key, tag.Value));
                }

                if (featurePropKeys.Count != featurePropValues.Count)
                {
                    throw new InvalidDataContractException("Property keys and values should have the same count");
                }

                var fData = new FeatureData
                {
                    Id = featureId,
                    GeometryType = (byte)GeometryType.Point,
                    Coordinates = (totalCoordinateCount, new List<Coordinate>
                    {
                        new Coordinate(node.Latitude, node.Longitude)
                    }),
                    PropertyKeys = (totalPropertyCount, featurePropKeys),
                    PropertyValues = (totalPropertyCount, featurePropValues)
                };
                featuresData.Add(featureId, fData);
                featureIds.Add(featureId);

                totalPropertyCount += featurePropKeys.Count;
                ++totalCoordinateCount;
            }

            offsets.Add(tileId, fileWriter.BaseStream.Position);

            // Write TileBlockHeader
            fileWriter.Write(featureIds.Count); // TileBlockHeader: FeatureCount
            fileWriter.Write(totalCoordinateCount); // TileBlockHeader: CoordinateCount
            fileWriter.Write(totalPropertyCount * 2); // TileBlockHeader: StringCount
            fileWriter.Write(0); //TileBlockHeader: CharactersCount

            // Take note of the offset within the file for this field
            var coPosition = fileWriter.BaseStream.Position;
            // Write a placeholder value to reserve space in the file
            fileWriter.Write((long)0); // TileBlockHeader: CoordinatesOffsetInBytes (placeholder)

            // Take note of the offset within the file for this field
            var soPosition = fileWriter.BaseStream.Position;
            // Write a placeholder value to reserve space in the file
            fileWriter.Write((long)0); // TileBlockHeader: StringsOffsetInBytes (placeholder)

            // Take note of the offset within the file for this field
            var choPosition = fileWriter.BaseStream.Position;
            // Write a placeholder value to reserve space in the file
            fileWriter.Write((long)0); // TileBlockHeader: CharactersOffsetInBytes (placeholder)

            // Write MapFeatures
            for (var i = 0; i < featureIds.Count; ++i)
            {
                var featureData = featuresData[featureIds[i]];

                fileWriter.Write(featureIds[i]); // MapFeature: Id
                fileWriter.Write(labels[i]); // MapFeature: LabelOffset
                fileWriter.Write(featureData.GeometryType); // MapFeature: GeometryType
                fileWriter.Write(featureData.Coordinates.offset); // MapFeature: CoordinateOffset
                fileWriter.Write(featureData.Coordinates.coordinates.Count); // MapFeature: CoordinateCount
                fileWriter.Write(featureData.PropertyKeys.offset * 2); // MapFeature: PropertiesOffset 
                fileWriter.Write(featureData.PropertyKeys.keys.Count); // MapFeature: PropertyCount
            }

            // Record the current position in the stream
            var currentPosition = fileWriter.BaseStream.Position;
            // Seek back in the file to the position of the field
            fileWriter.BaseStream.Position = coPosition;
            // Write the recorded 'currentPosition'
            fileWriter.Write(currentPosition); // TileBlockHeader: CoordinatesOffsetInBytes
            // And seek forward to continue updating the file
            fileWriter.BaseStream.Position = currentPosition;
            foreach (var t in featureIds)
            {
                var featureData = featuresData[t];

                foreach (var c in featureData.Coordinates.coordinates)
                {
                    fileWriter.Write(c.Latitude); // Coordinate: Latitude
                    fileWriter.Write(c.Longitude); // Coordinate: Longitude
                }
            }

            using var fileWriterFeatures = new StreamWriter(filePath.Replace(".bin", "_features.bin"));

            foreach (var t in featureIds)
            {
                var featureData = featuresData[t];

                fileWriterFeatures.WriteLine(featureData.PropertyKeys.keys.Count);
                for (var i = 0; i < featureData.PropertyKeys.keys.Count; ++i)
                {
                    fileWriterFeatures.WriteLine((int)featureData.PropertyKeys.keys[i]);
                    fileWriterFeatures.WriteLine((int)featureData.PropertyValues.values[i]);
                }
            }
        }

        // Seek to the beginning of the file, just before the first TileHeaderEntry
        fileWriter.Seek(Marshal.SizeOf<FileHeader>(), SeekOrigin.Begin);
        foreach (var (tileId, offset) in offsets)
        {
            fileWriter.Write(tileId);
            fileWriter.Write(offset);
        }

        fileWriter.Flush();
    }

    public static void Main(string[] args)
    {
        Options? arguments = null;
        var argParseResult =
            Parser.Default.ParseArguments<Options>(args).WithParsed(options => { arguments = options; });

        if (argParseResult.Errors.Any())
        {
            Environment.Exit(-1);
        }

        var mapData = LoadOsmFile(arguments!.OsmPbfFilePath);
        CreateMapDataFile(ref mapData, arguments!.OutputFilePath!);
    }

    public class Options
    {
        [Option('i', "input", Required = true, HelpText = "Input osm.pbf file")]
        public string? OsmPbfFilePath { get; set; }

        [Option('o', "output", Required = true, HelpText = "Output binary file")]
        public string? OutputFilePath { get; set; }
    }

    private readonly struct MapData
    {
        public ImmutableDictionary<long, AbstractNode> Nodes { get; init; }
        public ImmutableDictionary<int, List<long>> Tiles { get; init; }
        public ImmutableArray<Way> Ways { get; init; }
    }

    private struct FeatureData
    {
        public long Id { get; init; }

        public byte GeometryType { get; set; }
        public (int offset, List<Coordinate> coordinates) Coordinates { get; init; }
        public (int offset, List<int> keys) PropertyKeys { get; init; }
        public (int offset, List<int> values) PropertyValues { get; init; }
    }

    public static int ConvertKey(string str) {

        if (str == "highway") {
            return 1;
        }

        if (str == "highway:2020") {
            return 2;
        }

        if (str == "highway:lanes:backward") {
            return 3;
        }

        if (str == "highway:lanes:forward") {
            return 4;
        }

        if (str == "water") {
            return 5;
        }

        if (str == "waterway") {
            return 6;
        }

        if (str == "water_point") {
            return 7;
        }

        if (str == "railway") {
            return 8;
        }

        if (str == "natural") {
            return 9;
        }

        if (str == "boundary") {
            return 10;
        }

        if (str == "landuse") {
            return 11;
        }

        if (str == "building") {
            return 12;
        }

        if (str == "building:levels") {
            return 13;
        }

        if (str == "building:2020") {
            return 14;
        }

        if (str == "building:architecture") {
            return 15;
        }

        if (str == "leisure") {
            return 16;
        }
        
        if (str == "amenity") {
            return 17;
        }
        
        if (str == "amenity:2020") {
            return 18;
        }

        if (str == "admin_level") {
            return 19;
        }

        if (str == "place") {
            return 20;
        }

        if (str == "placement") {
            return 21;
        }

        if (str == "placement:forward") {
            return 22;
        }

        if (str == "name") {
            return 23;
        }

        return 0;
    }

    public static int ConvertValue(string key, string str) {

        if (str.StartsWith("motorway")) {
            return 1;
        }

        if (str.StartsWith("trunk")) {
            return 2;
        }

        if (str.StartsWith("primary")) {
            return 3;
        }

        if (str.StartsWith("secondary")) {
            return 4;
        }

        if (str.StartsWith("tertiary")) {
            return 5;
        }

        if (str.StartsWith("unclassified")) {
            return 6;
        }

        if (str.StartsWith("residential") && key == "highway") {
            return 7;
        }

        if (str.StartsWith("road")) {
            return 8;
        }

        if (str.StartsWith("administrative")) {
            return 9;
        }

        if (str.StartsWith("forest") && key.StartsWith("boundary")) {
            return 10;
        }

        if (str == "2") {
            return 11;
        }

        if (str.StartsWith("city")) {
            return 12;
        }

        if (str.StartsWith("town")) {
            return 13;
        }

        if (str.StartsWith("locality")) {
            return 14;
        }

        if (str.StartsWith("hamlet")) {
            return 15;
        }

        if (str == "fell") {
            return 16;
        }

        if (str == "grassland") {
            return 17;
        }

        if (str == "heath") {
            return 18;
        }

        if (str == "moor") {
            return 19;
        }

        if (str == "scrub") {
            return 20;
        }

        if (str == "wetland") {
            return 21;
        }

        if (str == "wood") {
            return 22;
        }

        if (str == "tree_row") {
            return 23;
        }

        if (str == "bare_rock") {
            return 24;
        }

        if (str == "rock") {
            return 25;
        }

        if (str == "scree") {
            return 26;
        }

        if (str == "beach") {
            return 27;
        }

        if (str == "sand") {
            return 28;
        }

        if (str == "water") {
            return 29;
        }
 
        if (str.StartsWith("forest") && key.StartsWith("landuse")) {
            return 30;
        }

        if (str.StartsWith("orchard")) {
            return 31;
        }

        if (str.StartsWith("residential") && key.StartsWith("landuse")) {
            return 32;
        }

        if (str.StartsWith("cemetery")) {
            return 33;
        }

        if (str.StartsWith("industrial")) {
            return 34;
        }

        if (str.StartsWith("commercial")) {
            return 35;
        }

        if (str.StartsWith("square")) {
            return 36;
        }

        if (str.StartsWith("construction")) {
            return 37;
        }

        if (str.StartsWith("military")) {
            return 38;
        }

        if (str.StartsWith("quarry")) {
            return 39;
        }

        if (str.StartsWith("brownfield")) {
            return 40;
        }

        if (str.StartsWith("farm")) {
            return 41;
        }

        if (str.StartsWith("meadow")) {
            return 42;
        }

        if (str.StartsWith("grass")) {
            return 43;
        }

        if (str.StartsWith("greenfield")) {
            return 44;
        }

        if (str.StartsWith("recreation_ground")) {
            return 45;
        }

        if (str.StartsWith("winter_sports")) {
            return 46;
        }

        if (str.StartsWith("allotments")) {
            return 47;
        }

        if (str.StartsWith("reservoir")) {
            return 48;
        }

        if (str.StartsWith("basin")) {
            return 49;
        }

        if (key == "name") {
            return 50;
        }

        return 0;
    }
}
