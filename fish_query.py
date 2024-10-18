"""Collect posts data from fishbrain."""

import argparse
import csv
import glob
import json
import os

import pygeoprocessing
import requests
import shapely
import shapely.wkt
import taskgraph
from osgeo import osr, ogr, gdal

BASE_URL = 'https://rutilus.fishbrain.com/graphql'


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


def query(bbox, cursor):
    # [minx, miny, maxx, maxy]
    variables = {
        "boundingBox": {
            "southWest": {
                "latitude": bbox[1],
                "longitude": bbox[0]
            },
            "northEast": {
                "latitude": bbox[3],
                "longitude": bbox[2]
            }
        },
        "first": 50,  # 50 seems to be max.
    }
    if cursor:
        variables['after'] = cursor

    query = "query GetCatchesInMapBoundingBox($boundingBox: BoundingBoxInputObject, $first: Int, $after: String, $caughtInMonths: [MonthEnum!], $speciesIds: [String!]) {\n  mapArea(boundingBox: $boundingBox) {\n    catches(\n      first: $first\n      after: $after\n      caughtInMonths: $caughtInMonths\n      speciesIds: $speciesIds\n    ) {\n      totalCount\n      pageInfo {\n        startCursor\n        hasNextPage\n        endCursor\n        __typename\n      }\n      edges {\n        node {\n          ...CatchId\n          createdAt\n          caughtAtGmt\n          post {\n            ...PostId\n            catch {\n              ...CatchId\n              ...CatchFishingWaterName\n              ...CatchSpeciesName\n              __typename\n            }\n            comments {\n              totalCount\n              __typename\n            }\n            createdAt\n            displayProductUnits {\n              totalCount\n              __typename\n            }\n            images(first: 1, croppingStrategy: SMART, height: 260, width: 333) {\n              totalCount\n              edges {\n                node {\n                  ...ImageFields\n                  __typename\n                }\n                __typename\n              }\n              __typename\n            }\n            likedByCurrentUser\n            likesCount\n            text {\n              text\n              __typename\n            }\n            user {\n              ...UserId\n              ...UserAvatar\n              nickname\n              __typename\n            }\n            __typename\n          }\n          species {\n            ...SpeciesId\n            displayName\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment CatchId on Catch {\n  _id: externalId\n  __typename\n}\n\nfragment CatchFishingWaterName on Catch {\n  fishingWater {\n    ...FishingWaterId\n    displayName\n    latitude\n    longitude\n    __typename\n  }\n  __typename\n}\n\nfragment FishingWaterId on FishingWater {\n  _id: externalId\n  __typename\n}\n\nfragment CatchSpeciesName on Catch {\n  species {\n    ...SpeciesId\n    displayName\n    __typename\n  }\n  __typename\n}\n\nfragment SpeciesId on Species {\n  _id: externalId\n  __typename\n}\n\nfragment PostId on Post {\n  _id: externalId\n  __typename\n}\n\nfragment ImageFields on Image {\n  width\n  height\n  url\n  __typename\n}\n\nfragment UserAvatar on User {\n  avatar(croppingStrategy: CENTER, height: 128, width: 128) {\n    height\n    width\n    ...ImageFields\n    __typename\n  }\n  __typename\n}\n\nfragment UserId on User {\n  _id: externalId\n  __typename\n}"
    r = requests.post(BASE_URL, json={'query': query, 'variables': variables})
    try:
        data = r.json()
    except Exception as err:
        print(r.text)
        raise err
    return data


def collect(bbox, target_filepath):
    print(f'Querying with bounding box: {bbox}')
    cursor = None
    data = query(bbox, cursor)
    total_count = data['data']['mapArea']['catches']['totalCount']
    print(f'found {total_count} catches to collect')

    if total_count >= 10000:
        print(f'{bbox} has more than 10000 catches. Only the first 10000'
              'can be queried. Consider choosing a smaller cellsize')

    collection = {
        'centroid_x': (bbox[0] + bbox[2]) / 2,
        'centroid_y': (bbox[1] + bbox[3]) / 2,
        'edges': []
    }
    has_next = True
    while has_next:
        data = query(bbox, cursor)
        collection['edges'].extend(data['data']['mapArea']['catches']['edges'])
        page_info = data['data']['mapArea']['catches']['pageInfo']
        cursor = page_info['endCursor']
        has_next = page_info['hasNextPage']
        print(f'Collected {len(collection["edges"])} catches...')
    with open(target_filepath, 'w') as file:
        file.write(json.dumps(collection))


def parse(json_list, target_filepath):
    base_record = {
        'centroid_x': '',
        'centroid_y': '',
        'id': '',
        'caughtAtGmt': '',
        'fishingWaterID': '',
        'fishingWaterName': '',
        'fishingWaterLon': '',
        'fishingWaterLat': '',
        'speciesID': '',
        'speciesName': '',
        'likesCount': '',
        'text': '',
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
            for item in collection['edges']:
                record = base_record.copy()
                node = item['node']
                record['centroid_x'] = collection['centroid_x']
                record['centroid_y'] = collection['centroid_y']
                record['id'] = node['_id']
                record['caughtAtGmt'] = node['caughtAtGmt']

                fishingWater = node['post']['catch'].get('fishingWater')
                if fishingWater:
                    record['fishingWaterID'] = fishingWater['_id']
                    record['fishingWaterName'] = fishingWater['displayName']
                    record['fishingWaterLon'] = fishingWater['longitude']
                    record['fishingWaterLat'] = fishingWater['latitude']

                species = node['post']['catch'].get('species')
                if species:
                    record['speciesID'] = species['_id']
                    record['speciesName'] = species['displayName']

                record['likesCount'] = node['post']['likesCount']
                record['text'] = node['post']['text']['text']
                record['userID'] = node['post']['user']['_id']
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
              'It will be divided into smaller polygons based on the cellsize argument.'))
    parser.add_argument(
        '-c', '--cellsize',
        type=int,
        help=('The AOI will be divided into square polygons with width and height '
              'equal to cellsize. Cellsize uses the same units as the AOI coordinate system.'))

    args = parser.parse_args(user_args)
    cache_dir = os.path.join(args.workspace, 'taskgraph_cache')
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    task_graph = taskgraph.TaskGraph(cache_dir, n_workers=-1)

    aoi_path = os.path.join(args.workspace, 'aoi.shp')
    grid_vector(args.aoi, args.cellsize, aoi_path)
    lat_lng_ref = osr.SpatialReference()
    lat_lng_ref.ImportFromEPSG(4326)  # EPSG 4326 is lat/lng
    aoi_path_wgs84 = os.path.join(args.workspace, 'aoi_wgs84.shp')
    pygeoprocessing.reproject_vector(
        aoi_path, lat_lng_ref.ExportToWkt(), aoi_path_wgs84)

    csv_filepath = os.path.join(args.workspace, 'catches.csv')
    json_dir = os.path.join(args.workspace, 'json')
    if not os.path.exists(json_dir):
        os.makedirs(json_dir)

    vector = gdal.OpenEx(aoi_path_wgs84)
    layer = vector.GetLayer()

    collect_task_list = []
    for feature in layer:
        fid = feature.GetFID()
        geom = feature.GetGeometryRef()
        envelope = geom.GetEnvelope()
        bbox = [envelope[0], envelope[2], envelope[1], envelope[3]]
        target_filepath = os.path.join(json_dir, f'{fid}.json')
        collect_task_list.append(task_graph.add_task(
            collect,
            args=[bbox, target_filepath],
            target_path_list=[target_filepath]))
    print('Completed queries.')

    json_list = glob.glob(os.path.join(json_dir, '*.json'))
    parse_task = task_graph.add_task(
        parse,
        args=[json_list, csv_filepath],
        target_path_list=[csv_filepath],
        dependent_task_list=collect_task_list)

    task_graph.close()
    task_graph.join()


if __name__ == '__main__':
    main()
