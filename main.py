import logging
import re
import glob
import pickle
from datetime import datetime

import flickr_api as fa
import pandas as pd
from tqdm import tqdm

from src.utils import start_loggers

today = datetime.now().strftime("%y%m%d")
complete_log = f"logs\\{today}_debug.log"
progress_log = f"logs\\progress.log"

# All logging statements go to complete_log, only logging.info statements go to progress_log
start_loggers(complete_log, progress_log)

# lookup tables
category_tags = pd.read_csv("data\\external\\sherlocknet_tags_csv\\sherlock_flickr_tags.csv", index_col="flickr_id")
rr_cat_tag_pkls = glob.glob("data\\external\\sherlocknet-tags\\tags\\*.pkl")  # rr = BL research repository
combined_rr_tag_dct = {}
for p in rr_cat_tag_pkls:
    with open(p, "rb") as f:
        tag_dct = pickle.load(f)
        combined_rr_tag_dct |= tag_dct


def parse_tags(tag_str, tag_re):
    """
    Parse a Flickr extras="machine_tag" string into category and tags
    Raise an error if any elements aren't recognised
    """
    tag_list = tag_str.split(" ")
    res = {"category": [None], "tag": []}
    for t in tag_list:
        snet, tag_type, text = tag_re.match(t).groupdict().values()
        if snet != "sherlocknet:" or tag_type[:-1] not in ["category", "tag"]:
            raise ValueError("Unknown machine tag type")
        else:
            res[type[:-1]].append(text)

    return res["category"].pop(), res["tag"]


# Get BL images
# *** This auth has WRITE permissions, take care ***
fa.set_auth_handler("flickr_api_session_auth_w.txt")
user = fa.Person(id='12403504@N02')  # BL id
photo_walker = fa.Walker(user.getPhotos, extras="machine_tags")
tag_re = re.compile(r"(?P<snet>[a-z]*:)(?P<type>[a-z]*=)(?P<text>[a-z]*(?=\Z))")

flkr_snet_cats = ["organism", "mammal", "landscapes", "architecture", "nature",
                  "objects", "maps", "diagrams", "seals", "text", "decorations", "miniatures"]

snet_tags_from_flkr = {k: {} for k in flkr_snet_cats}
flkr_rr_cat_mapping = {}

for i, p in tqdm(enumerate(photo_walker[:5]), total=len(photo_walker)):
    logging.info(f"Processing # {i} {p.id}")  # i is the (stable) position in the list of BL images
    if not p.machine_tags:  # skip any that don't have machine tags
        logging.debug(f"No machine tags for {p.id}")
    else:
        try:  # rr = BL research repository
            rr_cat, image_idx = category_tags.loc[int(p.id), ["tag", "image_idx"]]
        except KeyError:
            rr_cat, image_idx = None, None
        rr_tags_exist = image_idx in combined_rr_tag_dct
        logging.debug(f"IRO tags exist for {p.id}")

        flkr_cat, flkr_tags = parse_tags(p.machine_tags, tag_re)
        logging.debug(f"{p.id} has {len(flkr_tags)} machine tags")

        # Compare Flickr with RR to see if need to save any tags
        if flkr_cat and rr_cat:  # tags on Flickr and in the RR
            if flkr_cat != rr_cat:
                flkr_rr_cat_mapping[image_idx] = [flkr_cat, rr_cat]

            unmatched_tags = list(set(flkr_tags) - set([x[0] for x in combined_rr_tag_dct.get(image_idx, ["empty_set"])]))
            if rr_tags_exist and unmatched_tags:
                snet_tags_from_flkr[flkr_cat][image_idx] = unmatched_tags
            elif not rr_tags_exist:
                snet_tags_from_flkr[flkr_cat][image_idx] = unmatched_tags

        elif flkr_cat and not rr_cat:  # tags on Flickr but not in RR
            unmatched_tags = list(
                set(flkr_tags) - set([x[0] for x in combined_rr_tag_dct.get(image_idx, ["empty_set"])]))
            snet_tags_from_flkr[flkr_cat][image_idx] = unmatched_tags

        # Remove tags from Flickr
        sherlocknet_tags = [t for t in p.tags if "sherlocknet" in t.text]
        for st in sherlocknet_tags:
            logging.debug(f"Removing tag id={st.id} author={st.author.id} raw={st.raw}")
            try:
                # st.remove()
                logging.debug(f"Removed {st.id}")
            except:
                logging.error(f"Failed to remove {st.id}")

# TODO serialise saved tags as json in same format as research repository
