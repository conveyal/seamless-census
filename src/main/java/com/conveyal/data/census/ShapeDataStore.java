package com.conveyal.data.census;

import com.conveyal.data.geobuf.GeobufEncoder;
import com.conveyal.data.geobuf.GeobufFeature;
import com.vividsolutions.jts.geom.Envelope;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.zip.GZIPOutputStream;

/**
 * Store geographic data by ID, with index by zoom-11 tile.
 */
public class ShapeDataStore {
    public static final int ZOOM_LEVEL = 11;

    private static final Logger LOG = LoggerFactory.getLogger(ShapeDataStore.class);

    private DB db;

    /**
     * set of Object[] { int[] { x, y }, Feature } for features at zoom 11
     */
    private NavigableSet<Object[]> tiles;

    /**
     * Map from geoid to feature
     */
    private BTreeMap<Long, GeobufFeature> features;

    public ShapeDataStore() {
        db = DBMaker.tempFileDB().deleteFilesAfterClose().asyncWriteEnable()
                .asyncWriteFlushDelay(1000).make();

        features = db.treeMapCreate("features")
                .keySerializer(BTreeKeySerializer.LONG)
                .counterEnable()
                .make();

        tiles = db.treeSetCreate("tiles")
                .serializer(BTreeKeySerializer.ARRAY3)
                .make();

        // bind the map by tile
        features.modificationListenerAdd((id, feat0, feat1) -> {
            if (feat0 != null)
                // updates never change geometry, and there are no deletes
                return;

            // figure out which z11 tiles this is part of
            Envelope e = feat1.geometry.getEnvelopeInternal();
            for (int x = lon2tile(e.getMinX(), ZOOM_LEVEL); x <= lon2tile(e.getMaxX(), ZOOM_LEVEL); x++) {
                for (int y = lat2tile(e.getMaxY(), ZOOM_LEVEL); y <= lat2tile(e.getMinY(), ZOOM_LEVEL); y++) {
                    tiles.add(new Object[] {x, y, feat1.numericId});
                }
            }
        });
    }

    public void add(GeobufFeature feature) {
        if (this.features.containsKey(feature.numericId))
            throw new IllegalArgumentException("ID " + feature.numericId + " already present in store");
        this.features.put(feature.numericId, feature);

        if (this.features.size() % 10000 == 0)
            LOG.info("Loaded {} features", this.features.size());
    }

    /** Get the longitude of a particular tile */
    public static int lon2tile (double lon, int zoom) {
        // recenter
        lon += 180;

        // scale
        return (int) (lon * Math.pow(2, zoom) / 360);
    }

    /** Get the latitude of a particular tile */
    public static int lat2tile (double lat, int zoom) {
        // http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
        lat = Math.toRadians(lat);
        lat = Math.log(Math.tan(lat) + 1 / Math.cos(lat));

        return (int) ((1 - lat / Math.PI) / 2 * Math.pow(2, zoom));
    }

    /** Write GeoBuf tiles to a directory */
    public void writeTiles (File file) throws IOException {
        int lastx = -1, lasty = -1;

        List<GeobufFeature> featuresThisTile = new ArrayList<>();

        for (Object[] val : tiles) {
            int x = (int) val[0];
            int y = (int) val[1];
            long id = (long) val[2];

            if (x != lastx || y != lasty) {
                if (!featuresThisTile.isEmpty()) {
                    LOG.debug("x: {}, y: {}, {} features", lastx, lasty, featuresThisTile.size());
                    // write out the features
                    File dir = new File(file, "" + lastx);
                    File out = new File(dir, lasty + ".pbf.gz");
                    dir.mkdirs();
                    GeobufEncoder enc = new GeobufEncoder(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(out))), 12);
                    enc.writeFeatureCollection(featuresThisTile);
                    enc.close();
                    featuresThisTile.clear();
                }
            }

            featuresThisTile.add(features.get(id));

            lastx = x;
            lasty = y;
        }
    }

    /** get a feature */
    public GeobufFeature get(long id) {
        // protective copy, don't get entangled in mapdb async serialization.
        return features.get(id).clone();
    }

    /** put a feature that already exists */
    public void put (GeobufFeature feat) {
        if (!features.containsKey(feat.numericId))
            throw new IllegalArgumentException("Feature does not exist in database!");

        features.put(feat.numericId, feat);
    }
}
