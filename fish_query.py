"""Collect posts data from fishbrain."""

import argparse
import csv
import glob
import json
import os

import pandas
import pygeoprocessing
import requests
import taskgraph
from osgeo import osr, ogr

BASE_URL = 'https://rutilus.fishbrain.com/graphql'


def split_bbox(bbox):
    mid_x_coord = (bbox[0] + bbox[2]) / 2
    mid_y_coord = (bbox[1] + bbox[3]) / 2

    bounding_quads = [
        [bbox[0],  # xmin
         mid_y_coord,  # ymin
         mid_x_coord,  # xmax
         bbox[3]],
        [mid_x_coord,  # xmin
         mid_y_coord,  # ymin
         bbox[2],  # xmax
         bbox[3]],
        [bbox[0],  # xmin
         bbox[1],  # ymin
         mid_x_coord,  # xmax
         mid_y_coord],  # ymax
        [mid_x_coord,  # xmin
         bbox[1],  # ymin
         bbox[2],  # xmax
         mid_y_coord],  # ymax
    ]
    return bounding_quads

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


def collect(bbox, workspace, file_token):
    grand_total = 0

    def _collect(bbox, workspace):
        print(f'Querying with bounding box: {bbox}')
        cursor = None
        data = query(bbox, cursor)
        total_count = data['data']['mapArea']['catches']['totalCount']
        if total_count < 10000:
            print(f'found {total_count} catches to collect')
            nonlocal grand_total
            grand_total += total_count
            target_filepath = os.path.join(
                workspace, f'{"_".join([str(x) for x in bbox])}.json')
            results = []
            has_next = True
            while has_next:
                data = query(bbox, cursor)
                edges = data['data']['mapArea']['catches']['edges']
                results.extend(edges)
                page_info = data['data']['mapArea']['catches']['pageInfo']
                cursor = page_info['endCursor']
                has_next = page_info['hasNextPage']
                print(f'Collected {len(results)} catches...')
            with open(target_filepath, 'w') as file:
                file.write(json.dumps(results))
        else:
            quadrants = split_bbox(bbox)
            for quad in quadrants:
                _collect(quad, workspace)

    _collect(bbox, workspace)

    with open(file_token, 'w') as file:
        file.write(f'Collected {grand_total} catches in {bbox}')
    print('Completed queries.')


def parse(json_list, target_filepath):
    base_record = {
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
    with open(target_filepath, 'w', newline='') as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=fieldnames, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()

        for jsonfile in json_list:
            print(f'Parsing data in {jsonfile}')
            with open(jsonfile, 'r') as file:
                edges = json.load(file)
            for item in edges:
                record = base_record.copy()
                node = item['node']
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
        help=('The workspace in which outputs will be saved.'))
    parser.add_argument(
        '-a', '--aoi', default=None,
        help=('A polygon vector whose bounding box will be used for the query area. '
              'Required if extent is not used.'))
    parser.add_argument(
        '-b', '--bbox', default=None,
        help=('A bounding box that will be used for the query area.\n'
              'Format as a sequence of longitude/latitude decimal-degrees: \n'
              '--bbox min-lon min-lat max-lon max-lat \n'
              'Required if aoi is not used.'))

    args = parser.parse_args(user_args)
    cache_dir = os.path.join(args.workspace, 'taskgraph_cache')
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    task_graph = taskgraph.TaskGraph(cache_dir, n_workers=-1)

    if args.bbox:
        bbox = args.bbox
    elif args.aoi:
        lat_lng_ref = osr.SpatialReference()
        lat_lng_ref.ImportFromEPSG(4326)  # EPSG 4326 is lat/lng
        vector_info = pygeoprocessing.get_vector_info(args.aoi)
        native_bbox = vector_info['bounding_box']
        bbox = pygeoprocessing.transform_bounding_box(
            native_bbox, vector_info['projection_wkt'], lat_lng_ref.ExportToWkt())
    else:
        raise ValueError('Neither --aoi or --box was given as an argument.')

    csv_filepath = os.path.join(args.workspace, 'catches.csv')
    json_dir = os.path.join(args.workspace, 'json')
    file_token = os.path.join(json_dir, 'query_complete.txt')

    collection_task = task_graph.add_task(
        collect,
        args=[bbox, json_dir, file_token],
        target_path_list=[file_token])

    json_list = glob.glob(os.path.join(json_dir, '*.json'))
    parse_task = task_graph.add_task(
        parse,
        args=[json_list, csv_filepath],
        target_path_list=[csv_filepath],
        dependent_task_list=[collection_task])

    task_graph.close()
    task_graph.join()

if __name__ == '__main__':
    main()
