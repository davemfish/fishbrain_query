This script will query fishbrain for all catches and associated metadata within 
an area of interest.

Many catches are associated with a specific water body, and are geolocated 
using that waterbody. Some catches do not have a listed waterbody, but are 
still found within a spatial query. It is not clear where the location of these
catches is defined.

In order to get a more precise location for each catch, this script can divide
the area of interest into small grid cells, based on the `cellsize` argument.
The centroid of these gridcells is returned as part of each "catch" record, so
that each catch can be associated with a location, even if it is not associated
with a waterbody.


### Usage

#### Documentation

```
python fish_query.py --help
usage: fish_query.py [-h] [-w WORKSPACE] [-a AOI] [-c CELLSIZE]

optional arguments:
  -h, --help            show this help message and exit
  -w WORKSPACE, --workspace WORKSPACE
                        A directory in which outputs will be saved. This folder will be created if it does not exist.
  -a AOI, --aoi AOI     A polygon vector that defines the area to be queried. This vector can use any coordinate system. It will be divided into
                        smaller polygons based on the cellsize argument.
  -c CELLSIZE, --cellsize CELLSIZE
                        The AOI will be divided into square polygons with width and height equal to cellsize. Cellsize uses the same units as
                        the AOI coordinate system.
```

#### Example Usage
`python fish_query.py -w florida_grid2k --aoi aoi/AOI_Florida.shp --cellsize 2000`

### Outputs

`catches.csv` - the catch data in tabular format
`aoi.shp` - the gridded version of the area of interest
`aoi_wgs84.shp` - the AOI transformed into lat/lon coordinates
`json` - a directory with the raw data retrieved from fishbrain. This data is parsed into `catches.csv`.

#### `catches.csv` fields
+ centroid_x, centroid_y: the longitude and latitude of the center point of the grid cell
used to collect this record.
+id: a unique identifier for the "catch" or "post"
