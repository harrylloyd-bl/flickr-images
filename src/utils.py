import logging


def start_loggers(complete_log, progress_log):
    logging.basicConfig(filename=complete_log,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        encoding='utf-8',
                        level=logging.DEBUG)

    progress = logging.FileHandler(filename=progress_log)
    progress.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M')
    progress.setFormatter(formatter)
    logging.getLogger("").addHandler(progress)
