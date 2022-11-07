#! /usr/bin env python3
import asyncio
import datetime
import json
import os
import re
import uuid
from ast import literal_eval
from subprocess import Popen, PIPE, STDOUT

import aiohttp
import hug
import hvac
import requests
from bottle import run
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibmcloudant import CloudantV1
from ibmcloudant.cloudant_v1 import Document
from requests.auth import HTTPBasicAuth


def get_secret_from_vault(item_type, folder="bill"):
    client = hvac.Client(url="http://35.226.153.236:8200",
                         token=os.environ.get("VAULT_TOKEN", "s.ZTGM9Gz5Zla52dS85dPHNznS"))
    resp = client.read('cubbyhole/{}/'.format(folder))["data"][item_type]
    return resp


PROJECTS_LIST = get_secret_from_vault("projects", "oss_sync")
RCLONE_SERVER_DOMAIN = get_secret_from_vault("rclone_server", "oss_sync")
PRODUCTION_TOKEN = get_secret_from_vault("submission_token", "oss_sync")
DB_NAME = get_secret_from_vault("db_name", "oss_sync")
auth_async = aiohttp.BasicAuth(get_secret_from_vault("rclone_user", "oss_sync"),
                               get_secret_from_vault("rclone_pwd", "oss_sync"))
auth = HTTPBasicAuth(get_secret_from_vault("rclone_user", "oss_sync"),
                     get_secret_from_vault("rclone_pwd", "oss_sync"))

concurrency = int(get_secret_from_vault("concurrency", "oss_sync"))  # 最大并发量
semaphore = asyncio.Semaphore(concurrency)  # 信号量

authenticator = IAMAuthenticator(get_secret_from_vault("db_secret", "oss_sync"))
service = CloudantV1(authenticator=authenticator)
service.set_service_url(get_secret_from_vault("db_url", "oss_sync"))


def kill_seized_port(port):
    """
    重启服务进程
    :param port:
    :return:
    """
    p = Popen("lsof -i:{port}".format(port=port), shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT,
              close_fds=True)
    port_info = p.stdout.readlines()
    for item in port_info[1:]:
        pid = re.split(r"\s+", str(item))[1]
        p = Popen("kill -9 {pid}".format(pid=pid), shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        print(p.stdout.read())


def clear_cloudflare_cache(abstract):
    '''
    清除cloudflare cache
    :param abstract:
    :return:
    '''
    zone_identifier = get_secret_from_vault("zone_identifier", "cloudflare")
    api_key = get_secret_from_vault("api_key", "cloudflare")

    payload = {"purge_everything": True}
    headers = {"Content-Type": "application/json",
               "Authorization": "Bearer {}".format(api_key)
               }
    resp = requests.post("https://api.cloudflare.com/client/v4/zones/{}/purge_cache".format(zone_identifier),
                         data=json.dumps(payload), headers=headers).json()
    print(resp)
    print("=" * 100)
    if resp["success"] == True:
        msg = f"successfully to clear cloudflare cdn\n{abstract}"
        print(msg)
    else:
        msg = f"failed to clear cloudflare cdn\n{abstract}"
        print(msg)
    if str(get_secret_from_vault("cloudflare_clear_wechat_flag", "oss_sync")) == "1":
        try:
            requests.get("https://hidden-glitter-2824.bill-li.workers.dev?form=text&content={}".format(msg))
        except:
            pass
    return resp


def check_token(bucket, base_dir, token):
    if "production" in base_dir.lower() or base_dir == "/" or base_dir == "" \
            or base_dir.replace("/", "").endswith("ios") or base_dir.replace("/", "").endswith("android"):
        if token != PRODUCTION_TOKEN:
            return Exception("not allow to upload to {}/{}".format(bucket, base_dir))


def update_cloudant(db_name, job_id, **params):
    document_obj = service.get_document(db_name, doc_id=job_id).get_result()
    for key, value in params.items():
        document_obj[key] = value

    # Save the document in the database with "post_document" function
    create_document_response = service.put_document(
        db=db_name,
        doc_id=job_id,
        document=document_obj
    ).get_result()
    print(create_document_response)


def record_params(db_name, app_code, direct, base_paths, uploader, request_id, app_type, creation_time):
    '''
    app_code, direct, base_paths, uploader, request_id, app_type, creation_time
    :return:
    '''
    document_obj: Document = Document()
    document_obj.project = app_code
    document_obj.direction = direct
    document_obj.base_paths = base_paths
    document_obj.uploader = uploader
    document_obj.request_id = request_id
    document_obj.app_type = app_type
    document_obj.creation_time = creation_time
    create_document_response = service.post_document(
        db=db_name,
        document=document_obj
    ).get_result()
    print(create_document_response)


def record_cloudant(db_name, job_id, request_id, project, app_type, src_path, dst_path, direction, block_count,
                    uploader, path_type, start_time, end_time, error_msg="", duration="-1", status="",
                    server_response="", create_time=""):
    document_obj: Document = Document(id=job_id)

    document_obj.app_type = app_type
    document_obj.project = project
    document_obj.src_path = src_path
    document_obj.dst_path = dst_path
    document_obj.direction = direction
    document_obj.block_count = block_count
    document_obj.uploader = uploader
    document_obj.request_id = request_id
    document_obj.path_type = path_type
    document_obj.status = status
    document_obj.start_time = start_time
    document_obj.end_time = end_time
    document_obj.duration = duration
    document_obj.error_msg = error_msg
    document_obj.create_time = create_time
    document_obj.server_response = server_response

    # Save the document in the database with "post_document" function
    create_document_response = service.post_document(
        db=db_name,
        document=document_obj
    ).get_result()
    print(create_document_response)


async def create_job(app_code, path_type, app_type, direct, base_dir, uploader, block_count, request_id, token,
                     create_time):
    async with aiohttp.ClientSession() as session:
        async with session.get(PROJECTS_LIST, verify_ssl=False) as resp:
            projects_data = await resp.json()
            filtered_projects = [item for item in
                                 filter(lambda x: x["fields"]["app_code"] == app_code, projects_data["rows"])]
            if len(filtered_projects):
                aliyun_bucket = filtered_projects[0]["fields"]["aliyun_bucket"]
                gcp_bucket = filtered_projects[0]["fields"]["gcp_bucket"]
                backblaze_bucket = filtered_projects[0]["fields"]["backblaze_bucket"]

                src_pth = "{}/{}".format(aliyun_bucket, base_dir).replace("//", "/")
                dst_pth = f"{gcp_bucket}/{base_dir}" if direct == "ali2gcp" else f"{backblaze_bucket}/{base_dir}"
                dst_pth = dst_pth.replace("//", "/")
                check_token(gcp_bucket if direct == "ali2gcp" else backblaze_bucket, base_dir, token)

                if path_type.lower() == "folder":
                    url = f"http://{RCLONE_SERVER_DOMAIN}/sync/copy"
                    payload = {"srcFs": src_pth,
                               "dstFs": dst_pth}
                else:
                    url = f"http://{RCLONE_SERVER_DOMAIN}/operations/copyfile"
                    payload = {"srcFs": "{}:".format(src_pth.split(":")[0]),
                               "dstFs": "{}:".format(dst_pth.split(":")[0]),
                               "srcRemote": src_pth.split(":")[-1],
                               "dstRemote": dst_pth.split(":")[-1]}
                print(url)
                print(payload)
                async with semaphore:
                    # timeout = ClientTimeout(total=6000)
                    async with aiohttp.ClientSession() as client:
                        start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        job_id = str(uuid.uuid4()).replace("-", "") \
                                 + start_time.replace("-", "").replace(":", "").replace(" ", "")
                        record_cloudant(DB_NAME, job_id, request_id, app_code, app_type, src_pth, dst_pth,
                                        direct, block_count, uploader,
                                        path_type, start_time, "",
                                        error_msg="",
                                        duration="-1", status="syncing", server_response="init",
                                        create_time=create_time)
                        try:
                            payload.update({"_async": True})
                            async with client.post(url, data=payload,
                                                   auth=auth_async,
                                                   verify_ssl=False) as resp:
                                resp = await resp.text()
                                print(resp)

                            payload.update({"_async": False})
                            async with client.post(url, data=payload,
                                                   auth=auth_async,
                                                   verify_ssl=False) as resp:
                                await asyncio.sleep(10)
                                print(resp.status)
                                resp_json = await resp.text()
                                if resp.status == 200:
                                    print(resp.status)
                                    resp_json = resp_json.strip()
                                    resp_json = literal_eval(resp_json)
                                    print(resp_json)
                                    end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    if resp_json.get("error"):
                                        update_cloudant(DB_NAME, job_id, end_time=end_time,
                                                        error_msg=resp_json.get("error"),
                                                        duration="-1", status="failed", server_response=str(resp_json),
                                                        create_time=create_time)
                                        return resp_json
                                    else:
                                        update_cloudant(DB_NAME, job_id, end_time=end_time, error_msg="", duration="-1",
                                                        status="success", server_response=str(resp_json),
                                                        create_time=create_time)
                                        return resp_json
                                else:
                                    update_cloudant(DB_NAME, job_id, end_time="", error_msg="", duration="-1",
                                                    status="failed", server_response=str(resp_json),
                                                    create_time=create_time)

                        except asyncio.exceptions.TimeoutError as e:
                            update_cloudant(DB_NAME, job_id, end_time="",
                                            error_msg="request timeout", duration="-1", status="failed",
                                            server_response=str(e.args), create_time=create_time)
                        except Exception as e:
                            update_cloudant(DB_NAME, job_id, end_time="",
                                            error_msg=str(e.args), duration="-1", status="failed",
                                            server_response=str(e.args), create_time=create_time)
            else:
                raise Exception(f"Cannot find oss config of app {app_code}")


@hug.get('/')
def main(app_code, direct, base_paths, uploader, app_type, token=""):
    if not app_code or app_code == "":
        raise Exception()
    print("----- params -----")
    print(app_code, direct, base_paths, uploader, token, app_type)
    print("----- params -----")
    create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    request_id = str(uuid.uuid4())
    record_params("sync_param", app_code, direct, base_paths, uploader, request_id, app_type, create_time)
    loop = asyncio.get_event_loop()
    tasks = []
    if base_paths:
        blocks = base_paths.split(",")
        for _path in blocks:
            if "." in _path:
                path_type = "file"
            else:
                path_type = "folder"
            task = asyncio.ensure_future(
                create_job(app_code, path_type, app_type, direct, _path, uploader, len(blocks), request_id, token,
                           create_time))
            tasks.append(task)
        loop.run_until_complete(asyncio.wait(tasks))
        for _task in tasks:
            response = _task.result()
            print(response)
    else:
        back_dict = {"message": f"params::{base_paths} be blank", "status": "failed"}
        return back_dict
    if uploader.lower() == "submission":
        abstract = f"{app_code}::{direct}::{base_paths}"
        clear_cloudflare_cache(abstract)
    return {"response": "OK"}


if __name__ == "__main__":
    app = hug.API(__name__).http.server()
    run(app=app, reloader=True, host="0.0.0.0.0")

