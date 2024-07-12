"""
SOLR-indexer : Thumb Package, thumbnail-generator API call
================================

Copyright MET Norway

Licensed under the GNU GENERAL PUBLIC LICENSE, Version 3; you may not
use this file except in compliance with the License. You may obtain a
copy of the License at

    https://www.gnu.org/licenses/gpl-3.0.en.html

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.
"""

import requests
import logging

logger = logging.getLogger(__name__)


def create_wms_thumbnail_api(data: dict) -> dict:
    """Create a WMS thumbnail by calling the API given in the data dict"""
    headers = {'Content-Type': 'application/json'}
    host = data.get('host')
    endpoint = data.get('endpoint')
    url = host + endpoint
    del data['host']  # Remove field not part of request model.
    del data['endpoint']  # Remove field not part of request model.
    logger.debug("Calling wms thumbnail-generator API at: %s", url)
    result = {"data": {"url": None, "message": None, "task_id": None},
              "error": None,
              "status_code": None
              }

    try:
        logger.debug("calling API with json data: %s", data)
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xx

        # If the response status code is 200-299, no exception is raised
        resp = response.json()  # Get the response body
        logger.debug(resp)
        result.update({'data': resp.get('data')})
        result.update({'status_code': response.status_code})
        if response.status_code != 200:
            result.update({'error': resp.get('error')})
            logger.error("Could not create thumbnail task: %s", str(resp))

    except requests.HTTPError as e:
        # Log the error if an exception occurred
        # logger.error("Could not contact thumbnail genearator API: %s", str(e))
        result.update({"error": str(e)})
        result.update({"status_code": response.status_code})
        # raise e

    except Exception as e:
        # For any other exceptions
        # Log the error if an exception occurred
        # logger.error("Error generating thumbnail from API: %s", str(e))
        result.update({"error": str(e)})
        result.update({"status_code": response.status_code})
        # raise e

    finally:
        if result.get('error') is not None and result.get('task_id') is None:
            logger.debug("Error calling thumbnail API: %s", str(result.get('error')))
        return result
