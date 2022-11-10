#
#
# main() will be run when you invoke this action
#
# @param Cloud Functions actions accept a single parameter, which must be a JSON object.
#
# @return The output of this action, which must be a JSON object.
#
#
import base64
import hashlib
import json
import re

import hug
import requests
from bottle import run
from ibmcloudant import CloudantV1
from ibmcloudant.cloudant_v1 import Document
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator

authenticator = IAMAuthenticator("hT6T0_zVO5rsqC1YebqhHWBBEhdK6XPLj3w6iPCl3yOQ")

service = CloudantV1(authenticator=authenticator)
service.set_service_url("https://7bc2c5c9-91ca-45c5-9772-790c005b18e4-bluemix.cloudantnosqldb.appdomain.cloud")


def parse_item(item):
    blocks = item.split("/")
    if len(blocks) > 3:
        return {"key": "/".join(blocks[:4]), "value": "/".join(blocks[4:])}
    else:
        return {"key": item, "value": ""}


def splitpath(sample):
    split_data = map(lambda x: parse_item(x), sample)
    result = {}
    for item in split_data:
        if result.get(item["key"]):
            result[item["key"]].append(item["value"])
        else:
            result.update({item["key"]: [item["value"]]})
    result_list = []
    for key, value in result.items():
        result_list.append("【{}】".format(key))
        result_list.extend(sorted(value))

    return result_list


def record_jobs(db_name, request_id, params):
    '''
    app_code, direct, base_paths, uploader, request_id, app_type, creation_time
    :return:
    '''
    document_obj: Document = Document(id=request_id)
    for key, value in params.items():
        setattr(document_obj, key, value)
    create_document_response = service.post_document(
        db=db_name,
        document=document_obj
    ).get_result()
    print(create_document_response)


pattern = re.compile(r'<[^>]+>', re.S)


def query_cloudant(db_name, platform, limit, offset, app_code="", direction="", uploader=""):
    selector_dict = {}
    if platform and platform.upper() != "ALL":
        selector_dict.update({"platform": {"$eq": platform}})
    if app_code and app_code.upper() != "ALL":
        selector_dict.update({"project": {"$eq": app_code}})
    if uploader and uploader.upper() != "ALL":
        selector_dict.update({"uploader": {"$eq": uploader}})
    if direction and direction.upper() != "ALL":
        selector_dict.update({"direction": {"$eq": direction}})
    if limit and offset:
        response = service.post_find(db=db_name,
                                     selector=selector_dict,
                                     limit=limit,
                                     skip=offset,
                                     sort=[{"start_time": "desc"}]).get_result()[
            "docs"]
    else:
        response = service.post_find(db='job_list',
                                     selector=selector_dict,
                                     sort=[{"start_time": "desc"}]).get_result()[
            "docs"]

    return response


def get_headers():
    id_and_key = 'ec927faafae0:0021049303dbb037a1b417d29dc22c107a808a0fbf'
    basic_auth_string = 'Basic ' + base64.b64encode(bytes(id_and_key, "utf-8")).decode("utf-8")
    headers = {'Authorization': basic_auth_string}

    resp = requests.get('https://api.backblazeb2.com/b2api/v2/b2_authorize_account', headers=headers,
                        verify=False).json()

    api_url = resp["apiUrl"]  # Provided by b2_authorize_account
    account_authorization_token = resp["authorizationToken"]  # Provided by b2_authorize_account
    headers['Authorization'] = account_authorization_token
    account_id = resp["accountId"]
    return headers, api_url, account_id


def upload_file(file_data):
    bucket_id = '2e2cb962e7cf5a6a7f4a0e10'
    headers, api_url, account_id = get_headers()
    resp = requests.post(
        '%s/b2api/v2/b2_get_upload_url' % api_url,
        json.dumps({'bucketId': bucket_id}),
        headers=headers,
        verify=False
    ).json()
    upload_url = resp["uploadUrl"]
    upload_authorization_token = resp["authorizationToken"]  # Provided by b2_get_upload_url
    file_name = "jobs_list.json"
    content_type = "text/json"
    sha1_of_file_data = hashlib.sha1(file_data).hexdigest()

    headers = {
        'Authorization': upload_authorization_token,
        'X-Bz-File-Name': file_name,
        'Content-Type': content_type,
        'X-Bz-Content-Sha1': sha1_of_file_data,
        'X-Bz-Info-Author': 'unknown',
        'X-Bz-Server-Side-Encryption': 'AES256'
    }
    print(requests.post(url=upload_url, data=file_data, headers=headers, verify=False).json())


@hug.get('/')
def main(app_code="All", platform="All", direction="All", uploader="All", limit=None, offset=None):
    response = query_cloudant("job_list", platform, limit, offset, app_code=app_code, direction=direction,
                              uploader=uploader)

    return_data = {'response': response}
    upload_file(json.dumps(return_data).encode("utf-8"))
    return return_data


if __name__ == "__main__":
    app = hug.API(__name__).http.server()
    run(app=app, reloader=True, host="0.0.0.0", port="443")
