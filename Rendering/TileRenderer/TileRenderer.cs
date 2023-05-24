using Mapster.Common.MemoryMappedTypes;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.Drawing.Processing;
using SixLabors.ImageSharp.PixelFormats;
using SixLabors.ImageSharp.Processing;

namespace Mapster.Rendering;

public static class TileRenderer
{
    public static BaseShape Tessellate(this MapFeatureData feature, ref BoundingBox boundingBox, ref PriorityQueue<BaseShape, int> shapes)
    {
        BaseShape? baseShape = null;

        var featureType = feature.Type;
        if (feature.Properties.Any(p => (p.Key == 1) && 
                                    MapFeature.HighwayTypes.Any(v => p.Value == v)))
        {
            var coordinates = feature.Coordinates;
            var road = new Road(coordinates);
            baseShape = road;
            shapes.Enqueue(road, road.ZIndex);
        }
        else if (feature.Properties.Any(p => (p.Key == 5 || p.Key == 6 || p.Key == 7)) 
                                        && feature.Type != GeometryType.Point)
        {
            var coordinates = feature.Coordinates;

            var waterway = new Waterway(coordinates, feature.Type == GeometryType.Polygon);
            baseShape = waterway;
            shapes.Enqueue(waterway, waterway.ZIndex);
        }
        else if (Border.ShouldBeBorder(feature))
        {
            var coordinates = feature.Coordinates;
            var border = new Border(coordinates);
            baseShape = border;
            shapes.Enqueue(border, border.ZIndex);
        }
        else if (PopulatedPlace.ShouldBePopulatedPlace(feature))
        {
            var coordinates = feature.Coordinates;
            var popPlace = new PopulatedPlace(coordinates, feature);
            baseShape = popPlace;
            shapes.Enqueue(popPlace, popPlace.ZIndex);
        }
        else if (feature.Properties.Any(p => p.Key == 8))
        {
            var coordinates = feature.Coordinates;
            var railway = new Railway(coordinates);
            baseShape = railway;
            shapes.Enqueue(railway, railway.ZIndex);
        }
        else if (feature.Properties.Any(p => p.Key == 9 && featureType == GeometryType.Polygon))
        {
            var coordinates = feature.Coordinates;
            var geoFeature = new GeoFeature(coordinates, feature);
            baseShape = geoFeature;
            shapes.Enqueue(geoFeature, geoFeature.ZIndex);
        }
        else if (feature.Properties.Any(p => p.Key == 10 && p.Value == 10))
        {
            var coordinates = feature.Coordinates;
            var geoFeature = new GeoFeature(coordinates, GeoFeature.GeoFeatureType.Forest);
            baseShape = geoFeature;
            shapes.Enqueue(geoFeature, geoFeature.ZIndex);
        }
        else if (feature.Properties.Any(p => p.Key == 11 && (p.Value == 30 || p.Value == 31)))
        {
            var coordinates = feature.Coordinates;
            var geoFeature = new GeoFeature(coordinates, GeoFeature.GeoFeatureType.Forest);
            baseShape = geoFeature;
            shapes.Enqueue(geoFeature, geoFeature.ZIndex);
        }
        else if (feature.Type == GeometryType.Polygon && feature.Properties.Any(p
                     => p.Key == 11 && (p.Value == 32 || p.Value == 33 || p.Value == 34 || p.Value == 35 ||
                                                        p.Value == 36 || p.Value == 37 || p.Value == 38 || p.Value == 39 ||
                                                        p.Value == 40)))
        {
            var coordinates = feature.Coordinates;
            var geoFeature = new GeoFeature(coordinates, GeoFeature.GeoFeatureType.Residential);
            baseShape = geoFeature;
            shapes.Enqueue(geoFeature, geoFeature.ZIndex);
        }
        else if (feature.Type == GeometryType.Polygon && feature.Properties.Any(p
                     => p.Key == 11 && (p.Value == 41 || p.Value == 42 || p.Value == 43 || p.Value == 44 ||
                                                        p.Value == 45 || p.Value == 46 || p.Value == 47)))
        {
            var coordinates = feature.Coordinates;
            var geoFeature = new GeoFeature(coordinates, GeoFeature.GeoFeatureType.Plain);
            baseShape = geoFeature;
            shapes.Enqueue(geoFeature, geoFeature.ZIndex);
        }
        else if (feature.Type == GeometryType.Polygon &&
                 feature.Properties.Any(p => p.Key == 11 && (p.Value == 48 || p.Value == 49)))
        {
            var coordinates = feature.Coordinates;
            var geoFeature = new GeoFeature(coordinates, GeoFeature.GeoFeatureType.Water);
            baseShape = geoFeature;
            shapes.Enqueue(geoFeature, geoFeature.ZIndex);
        }
        else if (feature.Type == GeometryType.Polygon && feature.Properties.Any(p => (p.Key == 12 ||
                                                                                        p.Key == 13 ||
                                                                                        p.Key == 14 ||
                                                                                        p.Key == 15)))
        {
            var coordinates = feature.Coordinates;
            var geoFeature = new GeoFeature(coordinates, GeoFeature.GeoFeatureType.Residential);
            baseShape = geoFeature;
            shapes.Enqueue(geoFeature, geoFeature.ZIndex);
        }
        else if (feature.Type == GeometryType.Polygon && feature.Properties.Any(p => p.Key == 16))
        {
            var coordinates = feature.Coordinates;
            var geoFeature = new GeoFeature(coordinates, GeoFeature.GeoFeatureType.Residential);
            baseShape = geoFeature;
            shapes.Enqueue(geoFeature, geoFeature.ZIndex);
        }
        else if (feature.Type == GeometryType.Polygon && feature.Properties.Any(p => (p.Key == 17 ||
                                                                                        p.Key == 18)))
        {
            var coordinates = feature.Coordinates;
            var geoFeature = new GeoFeature(coordinates, GeoFeature.GeoFeatureType.Residential);
            baseShape = geoFeature;
            shapes.Enqueue(geoFeature, geoFeature.ZIndex);
        }

        if (baseShape != null)
        {
            for (var j = 0; j < baseShape.ScreenCoordinates.Length; ++j)
            {
                boundingBox.MinX = Math.Min(boundingBox.MinX, baseShape.ScreenCoordinates[j].X);
                boundingBox.MaxX = Math.Max(boundingBox.MaxX, baseShape.ScreenCoordinates[j].X);
                boundingBox.MinY = Math.Min(boundingBox.MinY, baseShape.ScreenCoordinates[j].Y);
                boundingBox.MaxY = Math.Max(boundingBox.MaxY, baseShape.ScreenCoordinates[j].Y);
            }
        }

        return baseShape;
    }

    public static Image<Rgba32> Render(this PriorityQueue<BaseShape, int> shapes, BoundingBox boundingBox, int width, int height)
    {
        var canvas = new Image<Rgba32>(width, height);

        // Calculate the scale for each pixel, essentially applying a normalization
        var scaleX = canvas.Width / (boundingBox.MaxX - boundingBox.MinX);
        var scaleY = canvas.Height / (boundingBox.MaxY - boundingBox.MinY);
        var scale = Math.Min(scaleX, scaleY);

        // Background Fill
        canvas.Mutate(x => x.Fill(Color.White));
        while (shapes.Count > 0)
        {
            var entry = shapes.Dequeue();
            // FIXME: Hack
            if (entry.ScreenCoordinates.Length < 2)
            {
                continue;
            }
            entry.TranslateAndScale(boundingBox.MinX, boundingBox.MinY, scale, canvas.Height);
            canvas.Mutate(x => entry.Render(x));
        }

        return canvas;
    }

    public struct BoundingBox
    {
        public float MinX;
        public float MaxX;
        public float MinY;
        public float MaxY;
    }
}
