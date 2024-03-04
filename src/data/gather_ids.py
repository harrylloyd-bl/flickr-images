import logging
import time
import sqlite3
from functools import partial
from tqdm import tqdm
import flickr_api as fa
from tqdm.contrib.concurrent import thread_map

log = "..\\..\\logs\\p_id_order_id_threading.log"
timing_log = "..\\..\\logs\\p_id_order_id_threading_timing.log"

logging.basicConfig(filename=timing_log,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    encoding='utf-8',
                    level=logging.DEBUG)

progress = logging.FileHandler(filename=log)
progress.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M')
progress.setFormatter(formatter)
logging.getLogger("").addHandler(progress)


with open(log, "r") as f:
    prog = f.readlines()
    clean_prog = [int(p.split(" ")[-2]) for p in prog]
    completed = len(list(set(clean_prog)))

# completed = 0

fa.set_auth_handler("..\\..\\flickr_api_session_auth_w.txt")
user = fa.Person(id='12403504@N02')
start_page = 1 + completed // 500
start_id = int(completed + 1 - ((completed + 1) // 500) * 500)
# walker = fa.Walker(user.getPhotos, extras="machine_tags", page=start_page, per_page=500)


def walker_generator(user, start_page, per_page, pages_per_walker, end_page):
    for p in range(start_page, end_page, pages_per_walker):
        yield fa.Walker(user.getPhotos, extras="machine_tags", page=p, per_page=per_page)


def log_pid(start_page, user, per_page, pages_per_walker, t0):
    walker = fa.Walker(user.getPhotos, extras="machine_tags", page=start_page, per_page=per_page)
    t_start, t_proc_start = round((time.perf_counter() - t0) * 1000), round((time.process_time()) * 1000)
    con = sqlite3.connect("..\\..\\data\\processed\\sherlocknet_tags.db")

    text = f"Pages #{walker._info.page} - {walker._info.page + pages_per_walker}, start: {t_start}"
    for p in walker[:per_page * pages_per_walker]:
        p_id_rec_time = round((time.perf_counter() - t0) * 1000)
        logging.info(f"{p.id} {p_id_rec_time}")

    t_end, t_proc_end = round((time.perf_counter() - t0) * 1000), round((time.process_time()) * 1000)

    timing_info = (int(walker._info.page), int(t_start), int(t_end), int(t_end - t_start), int(t_proc_end - t_proc_start))
    with con:
        con.execute("INSERT INTO thread_timing_log VALUES (?, ?, ?, ?, ?)", timing_info)
    con.close()


if __name__ == "__main__":
    pages_per_walker = 1
    per_page = 500
    # walk_gen = walker_generator(user, start_page=1, per_page=per_page, pages_per_walker=pages_per_walker, end_page=10)
    partial_log_pid = partial(log_pid, user=user, per_page=per_page, pages_per_walker=pages_per_walker, t0=time.perf_counter())

    # This thread_map is fast - about 20 mins for all 1m photos
    # thread_map(partial_log_pid, range(start_page, 3000, pages_per_walker), chunksize=1, total=3000 - completed//per_page)

    # This one's about 50s - 80% longer than the 30s for the threaded option
    # t0 = time.perf_counter()
    # for p in tqdm(walker[start_id:10000]):
    #     logging.info(f"{p.id } {round((time.perf_counter() - t0) * 1000)}")




