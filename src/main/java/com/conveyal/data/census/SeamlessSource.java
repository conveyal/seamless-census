package com.conveyal.data.census;

import com.conveyal.data.geobuf.GeobufDecoder;
import com.conveyal.data.geobuf.GeobufFeature;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.util.GeometricShapeFactory;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static com.conveyal.data.census.ShapeDataStore.lat2tile;
import static com.conveyal.data.census.ShapeDataStore.lon2tile;

/**
 * A tile source for seamless Census extracts
 */
public abstract class SeamlessSource {
    // convenience
    private static final int ZOOM_LEVEL = ShapeDataStore.ZOOM_LEVEL;

    protected static final Logger LOG = LoggerFactory.getLogger(SeamlessSource.class);

    private static final GeometryFactory geometryFactory = new GeometryFactory();

    public Map<Long, GeobufFeature> extract(double north, double east, double south, double west, boolean onDisk) throws
            IOException {
        Map<Long, GeobufFeature> ret;

        GeometricShapeFactory factory = new GeometricShapeFactory(geometryFactory);
        factory.setCentre(new Coordinate((east + west) / 2, (north + south) / 2));
        factory.setWidth(east - west);
        factory.setHeight(north - south);
        Polygon rect = factory.createRectangle();

        if (onDisk)
            ret = DBMaker.tempTreeMap();
        else
            ret = new HashMap<>();

        // read all the relevant tiles
        for (int x = lon2tile(west, ZOOM_LEVEL); x <= lon2tile(east, ZOOM_LEVEL); x++) {
            for (int y = lat2tile(north, ZOOM_LEVEL); y <= lat2tile(south, ZOOM_LEVEL); y++) {
                InputStream is = getInputStream(x, y);

                if (is == null)
                    // no data in this tile
                    continue;

                // decoder closes input stream as soon as it has read the tile
                GeobufDecoder decoder = new GeobufDecoder(new GZIPInputStream(new BufferedInputStream(is)));

                while (decoder.hasNext()) {
                    GeobufFeature f = decoder.next();
                    // blocks are duplicated at the edges of tiles, no need to import twice
                    if (ret.containsKey(f.numericId))
                        continue;

                    // confirm that it falls within the envelope
                    if (!rect.disjoint(f.geometry))
                        ret.put(f.numericId, f);
                }
            }
        }

        return ret;
    }

    /** get an input stream for the given tile */
    protected abstract InputStream getInputStream(int x, int y) throws IOException;
}
