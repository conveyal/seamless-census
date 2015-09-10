package com.conveyal.data.census;

import com.conveyal.data.geobuf.GeobufEncoder;
import com.conveyal.data.geobuf.GeobufFeature;

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
            System.err.println("usage: CensusExtractor (s3://bucket|data_dir) n e s w [outfile.json]");
        }

        SeamlessSource source;
        if (!args[0].startsWith("s3://"))
            source = new FileSeamlessSource(args[0]);
        else
            source = new S3SeamlessSource(args[0].substring(5));

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

        GeobufEncoder encoder = new GeobufEncoder(out, 12);
        encoder.writeFeatureCollection(features.values());
        encoder.close();

        if (out instanceof FileOutputStream)
            out.close();
    }
}
