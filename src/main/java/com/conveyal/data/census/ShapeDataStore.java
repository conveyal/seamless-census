package com.conveyal.data.census;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.conveyal.data.geobuf.GeobufEncoder;
import com.conveyal.data.geobuf.GeobufFeature;
import org.locationtech.jts.geom.Envelope;
import org.mapdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.zip.GZIPOutputStream;

/**
 * Store geographic data by ID, with index by zoom-11 tile.
 */
public class ShapeDataStore {
    public static final int ZOOM_LEVEL = 11;

    private static final Logger LOG = LoggerFactory.getLogger(ShapeDataStore.class);

    /** number of decimal places of precision to store */
    public static final int PRECISION = 12;

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
                .transactionDisable()
                .fileMmapEnable()
                .asyncWriteEnable()
                .asyncWriteFlushDelay(1000)
                .executorEnable()
                .asyncWriteQueueSize(10000)
                // start with 1GB
                .allocateStartSize(1024 * 1024 * 1024)
                // and bump by 512MB
                .allocateIncrement(512 * 1024 * 1024)
                .make();

        features = db.treeMapCreate("features")
                .keySerializer(BTreeKeySerializer.LONG)
                .valueSerializer(new GeobufEncoder.GeobufFeatureSerializer(12))
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
                    tiles.add(new Object[]{x, y, feat1.numericId});
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

    public void close () {
        db.close();
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
        writeTilesInternal((x, y) -> {
            // write out the features
            File dir = new File(file, "" + x);
            File out = new File(dir, y + ".pbf.gz");
            dir.mkdirs();
            return new FileOutputStream(out);
        });
    }

    /** Write GeoBuf tiles to S3 */
    public void writeTilesToS3 (String bucketName) throws IOException {
        // set up an upload thread
        ExecutorService executor = Executors.newSingleThreadExecutor();

        // initialize an S3 client
        AmazonS3 s3 =
                AmazonS3ClientBuilder.standard().build();
        try {
            writeTilesInternal((x, y) -> {
                PipedInputStream is = new PipedInputStream();
                PipedOutputStream os = new PipedOutputStream(is);
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentType("application/gzip");

                // perform the upload in its own thread so it doesn't deadlock
                executor.execute(() -> s3.putObject(bucketName, String.format("%d/%d.pbf.gz", x, y), is, metadata));
                return os;
            });
        } finally {
            // allow the JVM to exit
            executor.shutdown();
            try {
                executor.awaitTermination(1, TimeUnit.HOURS);
            } catch (InterruptedException e) {
                LOG.error("Interrupted while waiting for S3 uploads to finish");
            }
        }
    }

    /**
     * generic write tiles function, calls function with x and y indices to get an output stream, which it will close itself.
     * The Internal suffix is because lambdas in java get confused with overloaded functions
     */
    private void writeTilesInternal(TileOutputStreamProducer outputStreamForTile) throws IOException {
        int lastx = -1, lasty = -1, tileCount = 0;

        List<GeobufFeature> featuresThisTile = new ArrayList<>();

        for (Object[] val : tiles) {
            int x = (Integer) val[0];
            int y = (Integer) val[1];
            long id = (Long) val[2];

            if (x != lastx || y != lasty) {
                if (!featuresThisTile.isEmpty()) {
                    LOG.debug("x: {}, y: {}, {} features", lastx, lasty, featuresThisTile.size());
                    GeobufEncoder enc = new GeobufEncoder(new GZIPOutputStream(new BufferedOutputStream(outputStreamForTile.apply(lastx, lasty))), PRECISION);
                    enc.writeFeatureCollection(featuresThisTile);
                    enc.close();
                    featuresThisTile.clear();

                    tileCount++;
                }
            }

            featuresThisTile.add(features.get(id));

            lastx = x;
            lasty = y;
        }

        LOG.info("Wrote {} tiles", tileCount);
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

    @FunctionalInterface
    private interface TileOutputStreamProducer {
        public OutputStream apply (int x, int y) throws IOException;
    }
}
