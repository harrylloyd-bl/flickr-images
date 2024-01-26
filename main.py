import logging
import re
import sqlite3
import pickle
from datetime import datetime
import sys

import flickr_api as fa
import pandas as pd
from tqdm import tqdm

from src.utils import start_loggers

print("Initialising loggers")
today = datetime.now().strftime("%y%m%d")
complete_log = f"logs\\{today}_debug.log"
progress_log = f"logs\\progress.log"

# All logging statements go to complete_log, only logging.info statements go to progress_log
start_loggers(complete_log, progress_log)

print("Reading research repository tags")
# lookup tables
category_tags = pd.read_csv("data\\external\\sherlocknet_tags_csv\\sherlock_flickr_tags.csv", index_col="flickr_id")
with open("data\\external\\sherlocknet-tags\\tags\\combined_final.pkl", "rb") as f:
    combined_rr_tag_dct = pickle.load(f)


def parse_tags(tag_str, tag_re):
    """
    Parse a Flickr extras="machine_tag" string into category and tags
    Raise an error if any elements aren't recognised
    """
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


# Get BL images
# *** This auth has WRITE permissions, take care ***
print("Authenticating and getting photo walker")
with open("logs\\progress.log") as f:
    prog = f.readlines()
    clean_prog = [p.replace("  ", "").strip("\n").split(" ") for p in prog if "Processing" in p]
    completed = sorted(list(set([int(p[6]) for p in clean_prog if p[3] == "INFO"])))
    start_page = 1 + (max(completed) // 500)
    start_id = (max(completed) // 500) * 500

fa.set_auth_handler("flickr_api_session_auth_w.txt")
user = fa.Person(id='12403504@N02')  # BL id
# Either use Walker(page, per_page) or slice Walker to start at correct id.
# Walker slicing is badly implemented so page/per page is easier
photo_walker = fa.Walker(user.getPhotos, extras="machine_tags", page=start_page, per_page=500)

tag_re = re.compile(r"(?P<snet>[a-z]*:)(?P<type>[a-z]*=)(?P<text>[a-z]*(?=\Z))")

# tag and cat_map storage
con = sqlite3.connect("data\\processed\\sherlocknet_tags.db")
cur = con.cursor()

start, end = max(completed) - start_id, None
sl = slice(start, end)
print("Running through photo walker")
for i, p in tqdm(enumerate(photo_walker[sl]), total=len(photo_walker)):
    if not p.machine_tags:  # skip any that don't have machine tags
        logging.info(f"Processing # {i + start_id + start} {p.id}")  # i is the (stable) position in the list of BL images
        logging.debug(f"No machine tags for {p.id}")
    else:
        logging.info(f"Processing # {i + start_id + start} {p.id} T")  # i is the (stable) position in the list of BL images
        try:  # rr = BL research repository
            rr_cat, image_idx = category_tags.loc[int(p.id), ["tag", "image_idx"]]
        except KeyError:
            rr_cat, image_idx = None, None

        rr_tags = [x[0] for x in combined_rr_tag_dct.get(image_idx, [[None]])]
        logging.debug(f"{p.id} has {len(rr_tags)} IRO tags")

        flkr_cat, flkr_tags = parse_tags(p.machine_tags, tag_re)
        logging.debug(f"{p.id} has {len(flkr_tags)} machine tags")

        unmatched_tags = list(set(flkr_tags) - set(rr_tags))

        # Compare Flickr with RR to see if need to save any tags
        if flkr_cat != rr_cat:  # tags on Flickr and in the RR
            print(f"\n{p.id} Flickr category {flkr_cat} doesn't match RR {rr_cat}")
            logging.debug(f"{p.id} Flickr category {flkr_cat} doesn't match RR {rr_cat}")
            data = [int(p.id), int(image_idx), flkr_cat, rr_cat]
            try:
                cur.execute("INSERT OR IGNORE INTO catmap VALUES (?, ?, ?, ?)", data)
            except sqlite3.IntegrityError:
                logging.debug("Already stored")
                pass
            con.commit()

        if unmatched_tags:
            print(f"\n{p.id} has {len(unmatched_tags)} Flickr tags not in RR")
            logging.debug(f"{p.id} has {len(unmatched_tags)} Flickr tags not in RR")
            data = [(int(p.id), int(image_idx), flkr_cat, t) for t in unmatched_tags]
            cur.executemany("INSERT INTO tags VALUES (?, ?, ?, ?)", data)
            con.commit()

        # Remove tags from Flickr
        sherlocknet_tags = [t for t in p.tags if "sherlocknet" in t.text]
        logging.debug(f"Removing tags from {p.id}")
        for st in sherlocknet_tags:
            try:
                logging.debug(f"Removing tag id={st.id} author={st.author.id} raw={st.raw}")
                st.remove()  # comment out for safety during testing
            except:
                logging.error(f"Failed to remove {st.id}")
                logging.error(f"{repr(sys.exception())}")

con.close()
