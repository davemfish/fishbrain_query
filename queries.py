import requests

from requests.adapters import HTTPAdapter, Retry

BASE_URL = 'https://rutilus.fishbrain.com/graphql'

session = requests.Session()

retries = Retry(total=5,
                backoff_factor=0.1,
                status_forcelist=[500, 502, 503, 504])

session.mount('https://', HTTPAdapter(max_retries=retries))


def query_bounding_box(bbox, cursor):
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

    query = """
    query GetCatchesInMapBoundingBox($boundingBox: BoundingBoxInputObject, $first: Int, $after: String, $caughtInMonths: [MonthEnum!], $speciesIds: [String!]) {
      mapArea(boundingBox: $boundingBox) {
        catches(
          first: $first
          after: $after
          caughtInMonths: $caughtInMonths
          speciesIds: $speciesIds
        ) {
          totalCount
          pageInfo {
            startCursor
            hasNextPage
            endCursor
            __typename
          }
          edges {
            node {
              ...CatchId
              createdAt
              caughtAtGmt
              post {
                ...PostId
                catch {
                  ...CatchId
                  ...CatchFishingWaterName
                  ...CatchSpeciesName
                  __typename
                }
                comments {
                  totalCount
                  __typename
                }
                createdAt
                displayProductUnits {
                  totalCount
                  __typename
                }
                ...PostProductUnits
                ...PostVideoFields
                images(first: 1, croppingStrategy: SMART, height: 260, width: 333) {
                  totalCount
                  edges {
                    node {
                      ...ImageFields
                      __typename
                    }
                    __typename
                  }
                  __typename
                }
                likedByCurrentUser
                likesCount
                text {
                  text
                  __typename
                }
                user {
                  ...UserId
                  ...UserAvatar
                  nickname
                  __typename
                }
                __typename
              }
              species {
                ...SpeciesId
                displayName
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
    }

    fragment CatchId on Catch {
      _id: externalId
      __typename
    }

    fragment CatchFishingWaterName on Catch {
      fishingWater {
        ...FishingWaterId
        displayName
        latitude
        longitude
        __typename
      }
      __typename
    }

    fragment FishingWaterId on FishingWater {
      _id: externalId
      __typename
    }

    fragment CatchSpeciesName on Catch {
      species {
        ...SpeciesId
        displayName
        __typename
      }
      __typename
    }

    fragment SpeciesId on Species {
      _id: externalId
      __typename
    }

    fragment PostId on Post {
      _id: externalId
      __typename
    }

    fragment PostProductUnits on Post {
      productUnits(first: 20) {
        edges {
          node {
            ...ProductUnitId
            model
            image {
              ...ImageFields
              __typename
            }
            product {
              ...ProductId
              ...ProductBrandName
              displayName
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }

    fragment ImageFields on Image {
      width
      height
      url
      __typename
    }

    fragment ProductBrandName on Product {
      brand {
        ...BrandId
        name
        __typename
      }
      __typename
    }

    fragment BrandId on Brand {
      id
      __typename
    }

    fragment ProductId on Product {
      _id: externalId
      legacyId: id
      __typename
    }

    fragment ProductUnitId on ProductUnit {
      id
      __typename
    }

    fragment PostVideoFields on Post {
      video {
        ...VideoId
        hlsUrl
        webUrl
        originalUrl
        screenshot(width: 600) {
          ...ImageFields
          __typename
        }
        __typename
      }
      __typename
    }

    fragment VideoId on Video {
      id
      __typename
    }

    fragment UserAvatar on User {
      avatar(croppingStrategy: CENTER, height: 128, width: 128) {
        height
        width
        ...ImageFields
        __typename
      }
      __typename
    }

    fragment UserId on User {
      _id: externalId
      __typename
    }
    """
    r = session.post(BASE_URL, json={'query': query, 'variables': variables})
    try:
        data = r.json()
    except Exception as err:
        print(r.text)
        raise err
    return data


def query_catch(post_id):
    query = """
    query GetCatchDetails($externalId: String) {
      catchDetails: post(externalId: $externalId) {
        ...PostId
        catchConditions: catch {
          ...CatchId
          caughtAtLocalTimeZone
          latitude
          longitude
          moonIllumination
          sunPosition
          weatherAndMarineReading {
            ...WeatherAndMarineReadingId
            airPressure
            airTemperature
            waterTemperature
            weatherCondition {
              localizedValue
              worldWeatherOnlineIdentifier
              __typename
            }
            windDirection {
              degrees
              shortLocalizedValue
              __typename
            }
            windSpeed
            __typename
          }
          __typename
        }
        catchPost: catch {
          ...CatchId
          catchAndRelease
          caughtAtGmt
          createdAt
          fishingMethod {
            ...FishingMethodId
            displayName
            __typename
          }
          fishingWater {
            ...FishingWaterId
            displayName
            latitude
            longitude
            __typename
          }
          hasExactPosition
          length
          locationPrivacy
          species {
            ...SpeciesId
            displayName
            __typename
          }
          user {
            ...UserId
            avatar {
              url
              __typename
            }
            nickname
            __typename
          }
          weight
          __typename
        }
        trip {
          id
          catches {
            totalCount
            __typename
          }
          __typename
        }
        catchGear: displayProductUnits(first: 20) {
          edges {
            node {
              ...ProductUnitId
              product {
                ...ProductId
                brand {
                  ...BrandId
                  name
                  __typename
                }
                image {
                  ...ImageFields
                  __typename
                }
                displayName
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
    }

    fragment BrandId on Brand {
      id
      __typename
    }

    fragment CatchId on Catch {
      _id: externalId
      __typename
    }

    fragment FishingMethodId on FishingMethod {
      _id: externalId
      __typename
    }

    fragment FishingWaterId on FishingWater {
      _id: externalId
      __typename
    }

    fragment ImageFields on Image {
      width
      height
      url
      __typename
    }

    fragment SpeciesId on Species {
      _id: externalId
      __typename
    }

    fragment PostId on Post {
      _id: externalId
      __typename
    }

    fragment ProductId on Product {
      _id: externalId
      legacyId: id
      __typename
    }

    fragment ProductUnitId on ProductUnit {
      id
      __typename
    }

    fragment UserId on User {
      _id: externalId
      __typename
    }

    fragment WeatherAndMarineReadingId on WeatherAndMarineReading {
      id
      __typename
    }
    """
    variables = {
        'externalId': post_id
    }
    r = session.post(BASE_URL, json={'query': query, 'variables': variables})
    try:
        return r.json()
    except Exception as err:
        print(r.text)
        raise err
