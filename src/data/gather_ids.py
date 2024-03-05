import logging
import time
from ..utils import start_loggers
from functools import partial
import flickr_api as fa
from tqdm.contrib.concurrent import thread_map

log = "..\\..\\logs\\p_id_order_id_threading.log"
timing_log = "..\\..\\logs\\p_id_order_id_threading_timing.log"
start_loggers(timing_log, log)

with open(log, "r") as f:
    prog = f.readlines()
    clean_prog = [int(p.split(" ")[-2]) for p in prog]
    completed = len(list(set(clean_prog)))


fa.set_auth_handler("..\\..\\flickr_api_session_auth_w.txt")
user = fa.Person(id='12403504@N02')
start_page = 1 + completed // 500
start_id = int(completed + 1 - ((completed + 1) // 500) * 500)
# walker = fa.Walker(user.getPhotos, extras="machine_tags", page=start_page, per_page=500)


def log_pid(start_page, user, per_page, pages_per_walker, t0):
    walker = fa.Walker(user.getPhotos, extras="machine_tags", page=start_page, per_page=per_page)

    for p in walker[:per_page * pages_per_walker]:
        p_id_rec_time = round((time.perf_counter() - t0) * 1000)
        logging.info(f"{p.id} {p_id_rec_time}")


if __name__ == "__main__":
    pages_per_walker = 1
    per_page = 500
    partial_log_pid = partial(log_pid, user=user, per_page=per_page, pages_per_walker=pages_per_walker, t0=time.perf_counter())

    # This thread_map is fast - about 20 mins for all 1m photos
    thread_map(partial_log_pid, range(start_page, 3000, pages_per_walker), chunksize=1, total=3000 - completed//per_page)



