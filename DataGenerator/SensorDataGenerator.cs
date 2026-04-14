namespace DataGenerator;

public static class SensorDataGenerator
{
    public static string Generate(string dataType)
    {
        string normalizedType = dataType.Trim().ToUpperInvariant();

        return normalizedType switch
        {
            "TEMP" => NextDouble(12, 34).ToString("0.0", System.Globalization.CultureInfo.InvariantCulture),
            "HUM" => Random.Shared.Next(35, 91).ToString(),
            "RUIDO" => Random.Shared.Next(25, 101).ToString(),
            "PM2.5" => NextDouble(4, 60).ToString("0.0", System.Globalization.CultureInfo.InvariantCulture),
            "PM10" => NextDouble(8, 90).ToString("0.0", System.Globalization.CultureInfo.InvariantCulture),
            "LUM" => Random.Shared.Next(100, 1201).ToString(),
            _ => Random.Shared.Next(0, 101).ToString()
        };
    }

    private static double NextDouble(double min, double max)
    {
        return min + Random.Shared.NextDouble() * (max - min);
    }
}
