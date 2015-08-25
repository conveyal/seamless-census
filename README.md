# seamless-census

Import US Census data into a seamless storage environment.

## Usage

TODO

## Data storage

Data is stored in a directory structure, which is kept in Amazon S3. Census data is split
up into zoom-level-11 tiles and stored in [GeoBuf](https://github.com/mapbox/geobuf) files, each
in a directory for its source, its x coordinate and named its y coordinate.  For example, `us-census-2012/342/815.pbf`
might contain US LODES data and decennial census data for southeastern Goleta, CA.

Enumeration units that fall into two tiles should be included in both tiles. It is the responsibility
of the data consumer to deduplicate them; this can be done based on IDs. An enumeration unit that is
duplicated across tiles must have the same integer ID in both tiles.

## Use in Analyst Server

Any dataset that can be placed in this format can be used in Analyst Server.