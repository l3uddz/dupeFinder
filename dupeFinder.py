#!/usr/bin/env python3.5
import hashlib
import logging
import os
import pathlib
import sys
import timeit
from logging.handlers import RotatingFileHandler

import curio
from guessit import guessit

############################################################
# INIT
############################################################

# Setup logging
logFormatter = logging.Formatter('%(asctime)s - %(message)s')
rootLogger = logging.getLogger()

consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.DEBUG)
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

fileHandler = RotatingFileHandler('output.log', maxBytes=1024 * 1024 * 5, backupCount=5)
fileHandler.setLevel(logging.DEBUG)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

logger = rootLogger.getChild("DUPE_FINDER")
logger.setLevel(logging.DEBUG)

# Variables
files = {}
dupes = {}
unprocessed = []
non_videos = []


############################################################
# METHODS
############################################################

async def process_file(file):
    try:
        file_info = guessit(file.__str__())

        if file_info is not None:
            if 'mimetype' in file_info and 'video/' in file_info['mimetype']:
                # logger.debug("Processing file: %s", file)

                if file_info['type'] == 'episode':
                    if 'alternative_title' in file_info:
                        key = "{0}{1}".format(file_info['title'].lower(), file_info['alternative_title'].lower())
                    else:
                        key = "{0}".format(file_info['title'].lower())

                    if 'season' in file_info and 'episode' in file_info:
                        key += "{0}x{1}".format(file_info['season'], file_info['episode'])
                    elif 'date' in file_info:
                        key += str(file_info['date'])
                    else:
                        logger.debug("Not sure how to process this episode: %s", file)
                        unprocessed.append(file)
                        return None

                    if 'country' in file_info:
                        key += str(file_info['country'])

                    if 'year' in file_info:
                        key += str(file_info['year'])

                    if 'edition' in file_info:
                        key += str(file_info['edition'])

                elif file_info['type'] == 'movie':
                    key = file_info['title']
                    if 'year' in file_info:
                        key += str(file_info['year'])
                    if 'cd' in file_info:
                        key += str(file_info['cd'])

                else:
                    logger.debug("Not sure how to process: %s", file)
                    unprocessed.append(file)
                    return None

                key_hash = hashlib.md5(key.encode('utf-8')).hexdigest()
                if key_hash not in files:
                    # not a duplicate video
                    files[key_hash] = file
                else:
                    # duplicate video
                    logger.debug("Duplicate video found: %s", file)
                    dupes[file] = files[key_hash]

            else:
                non_videos.append(file)

    except Exception as ex:
        unprocessed.append(file)
        # logger.exception("Exception processing file: %s", file)
        return None


async def find_dupes(folder):
    logger.debug("Scanning folder: %s", folder)

    for path, subdirs, files in os.walk(folder):
        for name in files:
            file = pathlib.PurePath(path, name)
            await process_file(file.__str__())

    return None


############################################################
# MAIN ENTRY
############################################################

if __name__ == "__main__":
    folder = ''

    if len(sys.argv) < 2:
        logger.debug("You must specify a path, e.g. dupeFinder.py /home/seed/media/TV")
        exit()
    else:
        folder = sys.argv[1]

    start_time = timeit.default_timer()
    results = curio.run(find_dupes(folder), with_monitor=True)
    time_taken = timeit.default_timer() - start_time

    logger.debug("Finished looking for dupes!\n")
    logger.debug("Time taken: %d seconds", time_taken)
    logger.debug("Skipped non videos: %d", len(non_videos))
    logger.debug("Videos scanned: %d", len(files) + len(dupes))
    logger.debug("Non duplicates: %d", len(files))
    logger.debug("Duplicates: %d\n", len(dupes))

    if len(dupes):
        logger.debug("Dupes (Oldest):")
        for dupe, first in dupes.items():
            dupe_time = os.path.getmtime(dupe)
            first_time = os.path.getmtime(first)

            if dupe_time >= first_time:
                logger.debug("%s", first)
            else:
                logger.debug("%s", dupe)

    if len(unprocessed):
        logger.debug("Couldn't process:")
        for dupe in unprocessed:
            logger.debug(dupe)
