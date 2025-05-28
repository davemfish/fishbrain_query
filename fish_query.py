"""Collect posts data from fishbrain."""

import argparse
import csv
import json
import os

import pygeoprocessing
import shapely
import shapely.wkt
import taskgraph
from osgeo import osr, ogr, gdal

from queries import query_catch
from queries import query_bounding_box


def grid_vector(vector_path, cell_size, out_grid_vector_path):
    """Convert vector to a regular grid.

    Here the vector is gridded such that all cells are contained within the
    original vector.  Cells that would intersect with the boundary are not
    produced.

    Args:
        vector_path (string): path to an OGR compatible polygon vector type
        cell_size (float): dimensions of the grid cell in the projected units
            of ``vector_path``; if "square" then this indicates the side length,
            if "hexagon" indicates the width of the horizontal axis.
        out_grid_vector_path (string): path to the output ESRI shapefile
            vector that contains a gridded version of ``vector_path``, this file
            should not exist before this call

    Returns:
        None

    """
    driver = gdal.GetDriverByName('ESRI Shapefile')
    if os.path.exists(out_grid_vector_path):
        driver.Delete(out_grid_vector_path)

    vector = gdal.OpenEx(vector_path, gdal.OF_VECTOR)
    vector_layer = vector.GetLayer()
    spat_ref = vector_layer.GetSpatialRef()

    original_vector_shapes = []
    for feature in vector_layer:
        wkt_feat = shapely.wkt.loads(feature.geometry().ExportToWkt())
        original_vector_shapes.append(wkt_feat)
    vector_layer.ResetReading()
    original_polygon = shapely.prepared.prep(
        shapely.ops.unary_union(original_vector_shapes))

    out_grid_vector = driver.Create(
        out_grid_vector_path, 0, 0, 0, gdal.GDT_Unknown)
    grid_layer = out_grid_vector.CreateLayer(
        'grid', spat_ref, ogr.wkbPolygon)
    grid_layer_defn = grid_layer.GetLayerDefn()

    extent = vector_layer.GetExtent()  # minx maxx miny maxy

    def _generate_polygon(col_index, row_index):
        """Generate points for a closed square."""
        square = [
            (extent[0] + col_index * cell_size + x,
             extent[2] + row_index * cell_size + y)
            for x, y in [
                (0, 0), (cell_size, 0), (cell_size, cell_size),
                (0, cell_size), (0, 0)]]
        return square
    n_rows = int((extent[3] - extent[2]) / cell_size)
    n_cols = int((extent[1] - extent[0]) / cell_size)

    for row_index in range(n_rows):
        for col_index in range(n_cols):
            polygon_points = _generate_polygon(col_index, row_index)
            ring = ogr.Geometry(ogr.wkbLinearRing)
            for xoff, yoff in polygon_points:
                ring.AddPoint(xoff, yoff)
            poly = ogr.Geometry(ogr.wkbPolygon)
            poly.AddGeometry(ring)

            if original_polygon.contains(
                    shapely.wkt.loads(poly.ExportToWkt())):
                poly_feature = ogr.Feature(grid_layer_defn)
                poly_feature.SetGeometry(poly)
                grid_layer.CreateFeature(poly_feature)


def query_catch_details(in_json_filepath, centroid_dict, target_filepath):
    print('querying catch details...')
    data = {}
    with open(in_json_filepath, 'r') as file:
        collection = json.load(file)
    i = 0
    for item in collection['edges']:
        i += 1
        if (i > 0) and (i % 10 == 0):
            print(f'queried {i}')
        post_id = item['node']['post']['_id']
        results = query_catch(post_id)
        data[post_id] = results | centroid_dict

    print(f'collected details for {i} catches')
    with open(target_filepath, 'w') as file:
        file.write(json.dumps(data))


def collect(bbox, centroid_dict, target_filepath):
    print(f'Querying with bounding box: {bbox}')
    cursor = None
    data = query_bounding_box(bbox, cursor)
    total_count = data['data']['mapArea']['catches']['totalCount']
    print(f'found {total_count} catches to collect')

    if total_count >= 10000:
        print(f'{bbox} has more than 10000 catches. Only the first 10000'
              'can be queried. Consider choosing a smaller cellsize')

    collection = {'edges': []} | centroid_dict
    has_next = True
    while has_next:
        data = query_bounding_box(bbox, cursor)
        collection['edges'].extend(data['data']['mapArea']['catches']['edges'])
        page_info = data['data']['mapArea']['catches']['pageInfo']
        cursor = page_info['endCursor']
        has_next = page_info['hasNextPage']
        print(f'Collected {len(collection["edges"])} catches')
    with open(target_filepath, 'w') as file:
        file.write(json.dumps(collection))


def parse_catch_details(json_list, target_filepath):
    base_record = {
        'centroid_x': '',
        'centroid_y': '',
        'postID': '',
        'caughtAtGmt': '',
        'fishingWaterID': '',
        'fishingWaterName': '',
        'fishingWaterLon': '',
        'fishingWaterLat': '',
        'fishingMethod': '',
        'catchAndRelease': '',
        'speciesID': '',
        'speciesName': '',
        'length_meters': '',
        'weight_kg': '',
        'hasExactPosition': '',
        'locationPrivacy': '',
        'userID': ''
    }
    fieldnames = base_record.keys()
    with open(target_filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=fieldnames, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()

        for jsonfile in json_list:
            print(f'Parsing data in {jsonfile}')
            with open(jsonfile, 'r') as file:
                collection = json.load(file)
            for post_id, details in collection.items():
                record = base_record.copy()
                record['postID'] = post_id
                record['centroid_x'] = details['centroid_x']
                record['centroid_y'] = details['centroid_y']
                try:
                    data = details['data']['catchDetails']['catchPost']
                except (KeyError, TypeError) as e:
                    print(f'{post_id}: no details in {jsonfile}')
                    writer.writerow(record)
                    continue

                record['caughtAtGmt'] = data['caughtAtGmt']
                record['catchAndRelease'] = data['catchAndRelease']
                record['hasExactPosition'] = data['hasExactPosition']
                record['length_meters'] = data['length']
                record['weight_kg'] = data['weight']
                record['locationPrivacy'] = data['locationPrivacy']

                fishingWater = data.get('fishingWater')
                if fishingWater:
                    record['fishingWaterID'] = fishingWater['_id']
                    record['fishingWaterName'] = fishingWater['displayName']
                    record['fishingWaterLon'] = fishingWater['longitude']
                    record['fishingWaterLat'] = fishingWater['latitude']

                fishingMethod = data.get('fishingMethod')
                if fishingMethod:
                    record['fishingMethod'] = fishingMethod['displayName']

                species = data.get('species')
                if species:
                    record['speciesID'] = species['_id']
                    record['speciesName'] = species['displayName']

                user = data.get('user')
                if user:
                    record['userID'] = user['_id']

                writer.writerow(record)

    print(f'Completed. Tabular data is in {target_filepath}')


def main(user_args=None):
    parser = argparse.ArgumentParser(
        description=('')
    )
    parser.add_argument(
        '-w', '--workspace',
        help=('A directory in which outputs will be saved. '
              'This folder will be created if it does not exist.'))
    parser.add_argument(
        '-a', '--aoi',
        help=('A polygon vector that defines the area to be queried. '
              'This vector can use any coordinate system. '
              'It will be divided into smaller polygons if the cellsize '
              'argument is used.'))
    parser.add_argument(
        '-c', '--cellsize',
        type=int,
        help=('The AOI will be divided into square polygons with width and height '
              'equal to cellsize. Cellsize uses the same units as the AOI coordinate system. '
              'Omit this argument if the AOI should not be subdivided, for example, if it'
              'already contains a grid of square polygons.'))

    args = parser.parse_args(user_args)
    cache_dir = os.path.join(args.workspace, 'taskgraph_cache')
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    task_graph = taskgraph.TaskGraph(cache_dir, n_workers=-1)

    if args.cellsize:
        aoi_path = os.path.join(args.workspace, 'aoi.shp')
        _ = task_graph.add_task(
            grid_vector,
            args=(args.aoi, args.cellsize, aoi_path),
            target_path_list=[aoi_path])
        task_graph.join()
    else:
        aoi_path = args.aoi

    aoi_vector = gdal.OpenEx(aoi_path, gdal.OF_VECTOR)
    aoi_layer = aoi_vector.GetLayer()
    aoi_srs = aoi_layer.GetSpatialRef()
    lat_lng_ref = osr.SpatialReference()
    lat_lng_ref.ImportFromEPSG(4326)  # EPSG 4326 is lat/lng
    aoi_vector = aoi_layer = None
    if aoi_srs.IsSameGeogCS(lat_lng_ref):
        aoi_path_wgs84 = aoi_path
    else:
        aoi_path_wgs84 = os.path.join(args.workspace, 'aoi_wgs84.shp')
        _ = task_graph.add_task(
            pygeoprocessing.reproject_vector,
            args=(aoi_path, lat_lng_ref.ExportToWkt(), aoi_path_wgs84),
            target_path_list=[aoi_path_wgs84])
        task_graph.join()

    csv_filepath = os.path.join(args.workspace, 'catches.csv')
    json_dir = os.path.join(args.workspace, 'json')
    details_dir = os.path.join(json_dir, 'details')
    if not os.path.exists(json_dir):
        os.makedirs(json_dir)
    if not os.path.exists(details_dir):
        os.makedirs(details_dir)

    vector = gdal.OpenEx(aoi_path_wgs84)
    layer = vector.GetLayer()

    details_task_list = []
    target_details_path_list = []
    for feature in layer:
        fid = feature.GetFID()
        geom = feature.GetGeometryRef()
        envelope = geom.GetEnvelope()
        bbox = [envelope[0], envelope[2], envelope[1], envelope[3]]
        centroid_dict = {
            'centroid_x': (bbox[0] + bbox[2]) / 2,
            'centroid_y': (bbox[1] + bbox[3]) / 2
        }
        target_grid_filepath = os.path.join(json_dir, f'grid_{fid}.json')
        collect_task = task_graph.add_task(
            collect,
            args=[bbox, centroid_dict, target_grid_filepath],
            target_path_list=[target_grid_filepath])

        target_details_filepath = os.path.join(details_dir, f'details_{fid}.json')
        target_details_path_list.append(target_details_filepath)
        details_task = task_graph.add_task(
            query_catch_details,
            args=[target_grid_filepath, centroid_dict, target_details_filepath],
            target_path_list=[target_details_filepath],
            dependent_task_list=[collect_task])
        details_task_list.append(details_task)

    print('Completed queries.')

    parse_task = task_graph.add_task(
        parse_catch_details,
        args=[target_details_path_list, csv_filepath],
        target_path_list=[csv_filepath],
        dependent_task_list=details_task_list)

    task_graph.close()
    task_graph.join()


if __name__ == '__main__':
    main()
