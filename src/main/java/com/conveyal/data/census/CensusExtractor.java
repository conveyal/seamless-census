package com.conveyal.data.census;

import com.conveyal.data.geobuf.GeobufFeature;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * Extract Census data from a seamless datastore.
 */
public class CensusExtractor {
    public static void main (String... args) throws IOException {
        if (args.length != 5 && args.length != 6) {
            System.err.println("usage: CensusExtractor [s3://bucket/]file_prefix n e s w [outfile.json]");
        }

        SeamlessSource source = new FileSeamlessSource(args[0]);

        long start = System.currentTimeMillis();

        Map<Long, GeobufFeature> features = source.extract(Double.parseDouble(args[1]),
                Double.parseDouble(args[2]),
                Double.parseDouble(args[3]),
                Double.parseDouble(args[4]),
                false
                );

        OutputStream out;

        long completeTime = System.currentTimeMillis() - start;
        System.err.println("Read " + features.size() + " features in " + completeTime + "msec");

        if (args.length == 6)
            out = new FileOutputStream(new File(args[5]));
        else
            out = System.out;

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonGenerator gen = factory.createGenerator(out);

        // geojson header
        gen.writeStartObject();
        gen.writeStringField("type", "FeatureCollection");

        gen.writeArrayFieldStart("features");

        for (Map.Entry<Long, GeobufFeature> e : features.entrySet()) {
            GeobufFeature f = e.getValue();

            gen.writeStartObject();
            gen.writeStringField("type", "Feature");

            gen.writeObjectFieldStart("geometry");
            gen.writeStringField("type", "MultiPolygon");
            gen.writeArrayFieldStart("coordinates");

            MultiPolygon mp = (MultiPolygon) f.geometry;

            // for each polygon
            for (int poly = 0; poly < f.geometry.getNumGeometries(); poly++) {
                gen.writeStartArray();

                Polygon p = (Polygon) mp.getGeometryN(poly);

                // write the shell
                gen.writeStartArray();
                for (Coordinate coord : p.getExteriorRing().getCoordinates()) {
                    gen.writeStartArray();
                    gen.writeNumber(coord.x);
                    gen.writeNumber(coord.y);
                    gen.writeEndArray();
                }
                gen.writeEndArray();

                // write the rings
                for (int ring = 0; ring < p.getNumInteriorRing(); ring++) {
                    gen.writeStartArray();
                    for (Coordinate coord : p.getInteriorRingN(ring).getCoordinates()) {
                        gen.writeStartArray();
                        gen.writeNumber(coord.x);
                        gen.writeNumber(coord.y);
                        gen.writeEndArray();
                    }
                    gen.writeEndArray();
                }

                gen.writeEndArray();
            }

            gen.writeEndArray(); // coordinates
            gen.writeEndObject(); // geometry

            // write properties
            gen.writeObjectFieldStart("properties");
            for (Map.Entry<String, Object> prop : f.properties.entrySet()) {
                gen.writeFieldName(prop.getKey());

                Object val = prop.getValue();

                if (val instanceof Double || val instanceof Float)
                    gen.writeNumber(((Number) val).doubleValue());
                else if (val instanceof Long || val instanceof Integer)
                    gen.writeNumber(((Number) val).longValue());
            }

            gen.writeNumberField("id", f.numericId);

            gen.writeEndObject();

            gen.writeEndObject();
        }

        // don't forget this
        gen.writeEndArray();
        gen.writeEndObject();
        gen.close();

        out.flush();

        if (out instanceof FileOutputStream)
            out.close();
    }
}
