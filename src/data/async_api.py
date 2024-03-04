import asyncio
import sqlite3
from datetime import datetime
import logging
import re
import aiosqlite
import flickr_api as fa
from flickr_api.method_call import prep_api_args
from flickr_api.flickrerrors import FlickrServerError, FlickrAPIError
import httpx

print("\nInitialising loggers")
today = datetime.now().strftime("%y%m%d")
complete_log = f"..\\..\\logs\\{today}_debug_async.log"
error_log = f"..\\..\\logs\\error_async.log"

logging.basicConfig(filename=complete_log,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    encoding='utf-8',
                    level=logging.DEBUG)

errors = logging.FileHandler(filename=error_log)
errors.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M')
errors.setFormatter(formatter)
logging.getLogger("").addHandler(errors)

# All logging statements go to complete_log, only logging.error statements go to progress_log

# user = fa.Person(id='12403504@N02')
# walker = fa.Walker(user.getPhotos, extras="machine_tags", page=page, per_page=per_page)

# getPhotos_args = {'method': 'flickr.people.getPhotos', 'extras': 'machine_tags, tags', 'page': page, 'per_page': per_page, 'user_id': '12403504@N02'}
#
# rest_url = 'https://api.flickr.com/services/rest/'
# api_args = prep_api_args(auth_handler=fa.auth.AUTH_HANDLER, request_url=rest_url, **getPhotos_args)
#
# resp = requests.post(url=rest_url, data=api_args, timeout=20)
# print(api_args)


def parse_tags(tag_str):
    """
    Parse a Flickr extras="machine_tag" string into category and tags
    Raise an error if any elements aren't recognised
    """
    tag_re = re.compile(r"(?P<snet>[a-z]*:)(?P<type>[a-z]*=)(?P<text>[a-z]*(?=\Z))")
    tag_list = tag_str.split(" ")
    res = {"category": [None], "tag": []}
    for t in tag_list:
        if tag_re.match(t):
            ns, predicate, text = tag_re.match(t).groupdict().values()
            if ns == "sherlocknet:" and predicate[:-1] in ["category", "tag"]:
                res[predicate[:-1]].append(text)
            else:
                continue
        else:
            continue

    return res["category"].pop(), res["tag"]


# get a list of all images with sherlocknet machine tags
async def save_snet_tags(page, per_page, client, db):
    print(f"Page {page}")
    getPhotos_args = {'method': 'flickr.people.getPhotos', 'extras': 'machine_tags', 'page': page,
                      'per_page': per_page, 'user_id': '12403504@N02'}

    rest_url = 'https://api.flickr.com/services/rest/'
    api_args = prep_api_args(auth_handler=fa.auth.AUTH_HANDLER, request_url=rest_url, **getPhotos_args)

    try:
        resp = await client.post(url=rest_url, data=api_args, timeout=20)
        print(f"{page} {resp.status_code}")
    except httpx.PoolTimeout:
        logging.error(f"Page {page} failed due to timeout")
        return None

    if 500 <= resp.status_code < 600:
        logging.error(f"Page {page} failed due to FlickrServerError {resp.status_code}")
        # raise FlickrServerError(resp.status_code, resp.content.decode('utf8'))

    try:
        resp = resp.json()
    except ValueError as e:
        logging.error(f"Page {page} failed to parse response: {str(resp.content[:20])}")
        return None
        # print(f"Could not parse response: {str(resp.content)}")

    if resp["stat"] != "ok":
        logging.error(f"Page {page} failed due to FlickrAPIError {resp['code']} {resp['message']}")
        return None
        # raise FlickrAPIError(resp["code"], resp["message"])

    async with aiosqlite.connect(db) as db:
        for p in resp["photos"]["photo"]:
            flkr_cat = ''
            if p["machine_tags"]:
                flkr_cat, flkr_tags = parse_tags(p["machine_tags"])
            if flkr_cat:
                # print(f"p_id {p['id']}")
                tags = {"p_id": int(p["id"]), "flkr_cat": flkr_cat, "rr_tags": ' '.join(flkr_tags)}
                retry = 0
                while retry < 3:
                    try:
                        await db.execute("INSERT INTO async_flickr_tags VALUES(:p_id, :flkr_cat, :rr_tags) ON CONFLICT(p_id) DO UPDATE SET flkr_cat=:flkr_cat, t=:rr_tags WHERE p_id=:p_id", tags)
                        await db.commit()
                        break
                    except sqlite3.OperationalError:
                        retry += 1
                        print(f"retry {retry}")
                if retry == 3:
                    logging.error(f"{p['id']} failed due to db lock")
        print(f"{page} complete")


async def main(db, per_page):
    async with httpx.AsyncClient() as client:
        # @ 500 photos per page there are 2148 pages total
        # for i in range(1907, 2107, 25):
        tasks = []

        for page in range(2107, 2149):
            tasks.append(
                save_snet_tags(page=page, per_page=per_page, client=client, db=db)
            )

        await asyncio.gather(*tasks)


fa.set_auth_handler("..\\..\\flickr_api_session_auth_w.txt")

db = "..\\..\\data\\processed\\sherlocknet_tags.db"

if __name__ == "__main__":
    asyncio.run(main(db=db, per_page=500))


# do the comparisons with the dbs

# then separate step to remove tags

# confusingly named but this is the call to get Tags for a given photo
# photo_id = None
# getListPhoto_args = {'method': 'flickr.tags.getListPhoto', 'photo_id': f'{photo_id}'}
# async with httpx.AsyncClient() as client:
#     res = await client.get()


# get walker
# iterate through photos on walker, checking tags against what we have
# remove tags from each photo

# if not using flickr_api classes then
# call user.getPhotos
# return xml or json list of photos per page
# iterate through all sherlocknet tags and check against db
# won't have the convenience of stepping to next page so need to call that manually once done w a page

# have tag removal a seperate process